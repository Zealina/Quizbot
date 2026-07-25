from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional

from google import genai
from google.genai import types

from telegram import Message
from telegram.error import BadRequest


MODEL = "gemini-3.5-flash-lite"
USAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gemini_usage.json")

StatusCallback = Callable[[str], None]


@dataclass
class QuizConfig:
    topic_or_source: str = "Attached Document"
    page_range: str = "All"
    num_questions: int = 30
    num_options: int = 4
    difficulty: str = "Medium"
    languages: str = "English"
    question_types: str = "Mixed"

# --------------------------------------------------------------------------
# 2. JSON schema Gemini must fill in
# --------------------------------------------------------------------------

QUIZ_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "data": {
            "type": "STRING",
            "description": (
                "The complete, correctly formatted quiz text exactly as specified by the "
                "Required Format Template and all formatting rules in the instructions -- "
                "i.e. exactly what would have been output directly (inside a single code "
                "block) if this were not being returned as JSON."
            ),
        }
    },
    "required": ["data"],
}


def build_instruction_prompt(cfg: QuizConfig) -> str:
    return f"""**USER CONFIGURATION:**
* **Topic / Input Source:** {cfg.topic_or_source}
* **Page Number(s) / Range:** {cfg.page_range}
* **Number of Questions:** {cfg.num_questions}
* **Number of Options per Question:** {cfg.num_options}
* **Level of Difficulty:** {cfg.difficulty}
* **Language(s):** {cfg.languages}
* **Question Type(s):** {cfg.question_types}

**INSTRUCTIONS FOR AI:**
Act as an expert educational content extractor and quiz generator.
Based strictly on the "USER CONFIGURATION" above, process the input.

1. Intelligent Extraction vs. Generation (Document & Topic Handling):
* **If the attached document already contains MCQs or quiz questions:** Do NOT generate new ones. Extract ALL the existing questions (ignoring the number of questions stated above) and strictly reformat them to match the required template below.
* **If the attached document contains theory, notes, or standard informational text:** Analyze the text and generate high-quality, conceptual MCQs based on the content. Scale the complexity to match the requested **Level of Difficulty** and align the framing with standard, research-based Past Year Questions (PYQs) relevant to the subject.
* **If only a Topic is provided (no document):** Generate the questions using your internal knowledge base and deep research. Ensure the questions are highly relevant, strictly adhere to the **Level of Difficulty**, and reflect the pattern of competitive PYQs.

2. Source Material & Page Scope:
* If a "Page Number(s) / Range" is specified (and is not "All"), you MUST restrict your reading, extraction, and question generation strictly to those specific pages. Ignore the rest of the document.

3. Language Formatting (Bilingual Support):
* If a single language is specified, output everything in that language.
* If two languages are requested (bilingual), format EVERY line (Question, Statements, Options, Explanation) by separating the two languages with a forward slash ` / `. Example: `[Text in Lang 1] / [Text in Lang 2]`.

4. Question Types & Tables:
* Adapt to the requested question type(s).
* For "Match the following" questions, you MUST use a Markdown table to display the items to be matched directly under the question text.

5. Strict Output & Formatting Rules:
* Separate each distinct question from the next with a clear **double line break**.
* **NO GAPS WITHIN QUESTIONS:** Do not add any blank lines or empty spaces *inside* a single question. The question text, tables (if any), numbered statements/assertions, lettered options, and the explanation must appear on consecutive, unbroken lines.
* **NO QUESTION NUMBERS OR PREFIXES:** Do not include any question numbers or prefixes (e.g., remove Q1., Q., etc.). Start the block directly with the question text.
* **DYNAMIC OPTIONS:** Generate exactly the number of options requested in the configuration (e.g., A and B if 2; A, B, C, D if 4).
* Place the checkmark emoji (✅) immediately after the correct option text.
* Ensure the explanation always starts with "Ex: ".
MOST IMPORTANT: for mathematics quiz use $$ as GitHub stylings markdown


Required Format Template:

[Question Text (or Lang 1 / Lang 2)]
[Markdown Table if "Match the Following" - NO BLANK LINES AROUND IT]
1. [Statement/Assertion 1 if applicable (or Lang 1 / Lang 2)]
2. [Statement/Reason 2 if applicable (or Lang 1 / Lang 2)]
3. [Statement 3 if applicable (or Lang 1 / Lang 2)]
A) [Option A (or Lang 1 / Lang 2)]
B) [Option B (or Lang 1 / Lang 2)]
[... Continue up to the requested number of options] ✅
Ex: [Explanation detailing why the answer is correct (or Lang 1 / Lang 2)]

**FINAL OUTPUT WRAPPER:**
Return ONLY a single JSON object of the form {{"data": "..."}}, where the "data"
value is a single string containing ALL of the formatted questions produced per
the rules and template above, exactly as they would appear if they were not
wrapped in JSON (double line breaks between questions, no gaps within a
question, etc.). Do not output any conversational text, code fences, or
anything else outside of that JSON object.
"""


def parse_quiz_data(raw_text: str) -> str:
    """Strictly parses the model's response as {"data": "..."}.

    No partial/best-effort recovery anymore -- any malformed, incomplete,
    or unexpected response is treated as a plain error by the caller.
    Raises ValueError on any problem.
    """
    obj = json.loads(raw_text)  # raises json.JSONDecodeError on bad JSON
    data = obj.get("data")
    if not isinstance(data, str) or not data.strip():
        raise ValueError("Response JSON did not contain a non-empty 'data' string")
    return data.strip()


def _load_usage() -> dict:
    today = date.today().isoformat()
    if os.path.exists(USAGE_FILE):
        try:
            with open(USAGE_FILE) as f:
                data = json.load(f)
            if data.get("date") == today:
                return data
        except (json.JSONDecodeError, OSError):
            pass
    return {"date": today, "tokens_used": 0}


def _save_usage(data: dict) -> None:
    try:
        with open(USAGE_FILE, "w") as f:
            json.dump(data, f)
    except OSError:
        pass


def record_usage(tokens: int, daily_limit: int) -> dict:
    data = _load_usage()
    data["tokens_used"] += max(tokens, 0)
    _save_usage(data)
    used = data["tokens_used"]
    pct = round(100 * used / daily_limit, 1) if daily_limit else 0.0
    return {"tokens_used_today": used, "daily_limit": daily_limit, "percent_used": pct}


def _with_retries(fn, *, retries=3, base_delay=2, status_callback: Optional[StatusCallback] = None, label="request"):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                delay = base_delay * (2 ** (attempt - 1))
                if status_callback:
                    status_callback(f"⚠️ {label} failed (attempt {attempt}/{retries}): {exc}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                if status_callback:
                    status_callback(f"❌ {label} failed after {retries} attempts: {exc}")
    raise last_exc


async def _with_retries_async(fn, *, retries=3, base_delay=2, notify=None, label="request"):
    """Same backoff/retry behavior as `_with_retries`, but for an async `fn`
    and an async `notify` callback (so status updates can be awaited)."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                delay = base_delay * (2 ** (attempt - 1))
                if notify:
                    await notify(f"⚠️ {label} failed (attempt {attempt}/{retries}): {exc}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                if notify:
                    await notify(f"❌ {label} failed after {retries} attempts: {exc}")
    raise last_exc


async def generate_quiz(
    cfg: QuizConfig = QuizConfig(),
    file_path: Optional[str] = None,
    api_key: Optional[str] = None,
    max_output_tokens: int = 20000,
    daily_token_limit: int = 1_000_000,
    status_callback: Optional[StatusCallback] = None,
    retries: int = 3,
    tgm_message: Optional[Message] = None,
    no_of_questions = 20
) -> dict[str, Any]:
    status_state: dict[str] = {"last_text": None}
    cfg.num_questions = no_of_questions

    async def notify(msg: str):
        if status_callback:
            status_callback(msg)

        if tgm_message is None or msg == status_state["last_text"]:
            return
        status_state["last_text"] = msg

        try:
            await tgm_message.edit_text(msg)
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                pass

    def notify_fire_and_forget(msg: str):
        asyncio.create_task(notify(msg))

    client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
    contents: list[Any] = []

    if file_path:
        file_name, file_extension = os.path.splitext(file_path)
        if file_extension == ".docx":
            file_path = convert_docx_to_txt(file_path)

            print(file_path, "...just created")

        await notify(f"📄 Uploading document ({os.path.basename(file_path)})...")
        try:
            uploaded = _with_retries(
                lambda: client.files.upload(file=file_path),
                retries=retries, status_callback=notify_fire_and_forget, label="Document upload",
            )
            contents.append(uploaded)
            await notify("✅ Document uploaded.")
        except Exception as exc:
            return {
                "status": "error",
                "error": f"Upload failed: {exc}",
                "model": MODEL,
                "config": cfg.__dict__,
                "formatted_text": "",
            }

    contents.append(build_instruction_prompt(cfg))

    gen_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=QUIZ_SCHEMA,
        max_output_tokens=max_output_tokens,
        temperature=0.4,
    )

    est_tokens_per_question = 120 + (40 * cfg.num_options)
    estimated_total_tokens = cfg.num_questions * est_tokens_per_question + 100

    await notify(f"🧠 Formatting with {MODEL}...")

    start = time.time()
    accumulated_text = ""
    finish_reason = None
    usage_metadata = None
    state = {"last_notify": start}

    async def run_stream():
        nonlocal accumulated_text, finish_reason, usage_metadata
        stream = client.models.generate_content_stream(model=MODEL, contents=contents, config=gen_config)
        for chunk in stream:
            if getattr(chunk, "text", None):
                accumulated_text += chunk.text
            if getattr(chunk, "usage_metadata", None):
                usage_metadata = chunk.usage_metadata
            if chunk.candidates and chunk.candidates[0].finish_reason:
                finish_reason = str(chunk.candidates[0].finish_reason)

            now = time.time()
            if now - state["last_notify"] >= 2.5:
                elapsed = now - start
                approx_tokens = max(len(accumulated_text) // 4, 1)
                rate = approx_tokens / elapsed if elapsed > 0 else 0
                remaining = max(estimated_total_tokens - approx_tokens, 0)
                eta_str = f"~{round(remaining / rate)}s remaining" if rate > 0 else "estimating time..."
                await notify(f"⏱ {round(elapsed)}s elapsed | {eta_str} | ~{approx_tokens} tokens generated so far")
                state["last_notify"] = now

    error = None
    try:
        await _with_retries_async(run_stream, retries=retries, notify=notify, label="Gemini generation")
    except Exception as exc:
        error = str(exc)

    elapsed_total = round(time.time() - start, 2)

    total_tokens_this_call = (
        usage_metadata.total_token_count if usage_metadata and hasattr(usage_metadata, "total_token_count")
        else max(len(accumulated_text) // 4, 0)
    )
    usage_report = record_usage(total_tokens_this_call, daily_token_limit)

    async def graceful_error(message: str) -> dict[str, Any]:
        await notify(f"❌ {message}")
        return {
            "status": "error",
            "error": message,
            "model": MODEL,
            "config": cfg.__dict__,
            "generation_time_seconds": elapsed_total,
            "finish_reason": finish_reason,
            "tokens_used_this_request": total_tokens_this_call,
            "usage_today": usage_report,
            "formatted_text": "",
        }

    if error:
        return await graceful_error(f"Gemini API appears unavailable: {error}")

    if not accumulated_text:
        return await graceful_error("Gemini returned an empty response.")

    if finish_reason is not None and "MAX_TOKENS" in finish_reason:
        return await graceful_error("Response was cut off before completion (max tokens reached).")

    try:
        data_text = parse_quiz_data(accumulated_text)
    except (json.JSONDecodeError, ValueError) as exc:
        return await graceful_error(f"Could not parse a valid response: {exc}")

    await notify(
        f"✅ Done in {elapsed_total}s — quiz formatted successfully.\n"
        f"📊 Tokens used this request: {total_tokens_this_call:,}. "
        f"Today's usage: {usage_report['tokens_used_today']:,} / {usage_report['daily_limit']:,} "
        f"({usage_report['percent_used']}%)."
    )

    return {
        "status": "complete",
        "error": None,
        "model": MODEL,
        "config": cfg.__dict__,
        "generation_time_seconds": elapsed_total,
        "finish_reason": finish_reason,
        "tokens_used_this_request": total_tokens_this_call,
        "usage_today": usage_report,
        "formatted_text": f"{data_text}",
    }

def convert_docx_to_txt(file_path):
    
    from docx import Document

    doc = Document(file_path)
    text = []
    
    for paragraph in doc.paragraphs:
        if paragraph.text.strip():
            text.append(paragraph.text)
    joined_text = "\n".join(text)
    
    file_name, file_extension = os.path.splitext(file_path)

    with open(f"{file_name}.txt", "w+") as fp:
        fp.write(joined_text)

    if os.path.exists(file_path):
        os.remove(file_path)
    return f"{file_name}.txt"
