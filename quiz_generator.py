"""
Gemini-powered MCQ Generator
=============================
Takes a document (PDF/image) or a bare topic, formats it according to the
"USER CONFIGURATION" quiz-generation spec, calls Gemini 2.5 Flash-Lite, and
ALWAYS returns a well-formed JSON object -- even if the model gets cut off
by the token limit or the API errors out mid-request.

At every meaningful step it emits a plain human-readable status string via
`status_callback` (e.g. "Uploading document...", "12s elapsed, ~5s left",
"Tokens used today: 4,200 / 1,000,000"). Pipe that callback straight into
your Telegram bot's send_message().

Install:
    pip install google-genai

Set your key:
    export GEMINI_API_KEY="..."

Run:
    python quiz_generator.py --topic "Newton's Laws" --num-questions 5 \
        --num-options 4 --difficulty Hard --language English --qtype Mixed
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Optional

from google import genai
from google.genai import types

MODEL = "gemini-2.5-flash-lite"
USAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gemini_usage.json")

StatusCallback = Callable[[str], None]

# --------------------------------------------------------------------------
# 1. Config
# --------------------------------------------------------------------------

@dataclass
class QuizConfig:
    topic_or_source: str = "Attached Document"
    page_range: str = "All"
    num_questions: int = 5
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
        "questions": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "question_text": {"type": "STRING"},
                    "table_markdown": {
                        "type": "STRING",
                        "nullable": True,
                        "description": "Markdown table, only for 'Match the following' questions, else null",
                    },
                    "statements": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "Numbered statements/assertion-reason lines, empty array if not applicable",
                    },
                    "options": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "label": {"type": "STRING", "description": "A, B, C..."},
                                "text": {"type": "STRING"},
                                "is_correct": {"type": "BOOLEAN"},
                            },
                            "required": ["label", "text", "is_correct"],
                        },
                    },
                    "explanation": {
                        "type": "STRING",
                        "description": "Always starts with 'Ex: '",
                    },
                },
                "required": ["question_text", "options", "explanation"],
            },
        }
    },
    "required": ["questions"],
}


def build_instruction_prompt(cfg: QuizConfig) -> str:
    return f"""Act as an expert educational content extractor and quiz generator.

**USER CONFIGURATION:**
- Topic / Input Source: {cfg.topic_or_source}
- Page Number(s) / Range: {cfg.page_range}
- Number of Questions: {cfg.num_questions}
- Number of Options per Question: {cfg.num_options}
- Level of Difficulty: {cfg.difficulty}
- Language(s): {cfg.languages}
- Question Type(s): {cfg.question_types}

**INSTRUCTIONS:**
1. Intelligent Extraction vs. Generation:
   - If the attached document already contains MCQs/quiz questions, do NOT invent new
     ones -- extract and reformat the existing ones into the schema.
   - If the document contains theory/notes, generate high-quality conceptual MCQs
     scaled to the requested difficulty, styled like research-based PYQs.
   - If only a topic is given (no document), generate from your own knowledge,
     matching the difficulty and PYQ-style patterns for that subject.
2. Page Scope: if Page Number(s)/Range is not "All", restrict reading/extraction
   strictly to those pages and ignore the rest of the document.
3. Bilingual formatting: if Language(s) contains two languages separated by "/",
   every text field (question_text, each statement, each option text, explanation)
   must itself contain both languages separated by " / ". If a single language is
   given, use only that language.
4. Question types: adapt to the requested type(s). For "Match the following"
   questions, put the items to match into table_markdown as a Markdown table;
   leave table_markdown null for all other question types.
5. Produce exactly {cfg.num_questions} questions, each with exactly {cfg.num_options}
   options labelled A, B, C... Exactly one option per question must have
   is_correct = true. explanation must always start with "Ex: ".
6. Do not include question numbers/prefixes inside question_text.

Return ONLY the JSON object described by the response schema. No prose outside it.
"""


# --------------------------------------------------------------------------
# 3. Truncation-safe JSON extraction
# --------------------------------------------------------------------------

def extract_complete_objects(raw_text: str) -> list[dict]:
    """Returns every fully-closed question object inside the `questions`
    array, even if the response as a whole was cut off mid-stream."""
    start = raw_text.find("[")
    if start == -1:
        return []

    depth = 0
    obj_start: Optional[int] = None
    in_string = False
    escape = False
    completed: list[dict] = []

    i = start + 1
    n = len(raw_text)
    while i < n:
        c = raw_text[i]
        if in_string:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == "{":
                if depth == 0:
                    obj_start = i
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0 and obj_start is not None:
                    chunk = raw_text[obj_start:i + 1]
                    try:
                        completed.append(json.loads(chunk))
                    except json.JSONDecodeError:
                        pass
                    obj_start = None
            elif c == "]" and depth == 0:
                break
        i += 1
    return completed


def parse_quiz_json(raw_text: str) -> tuple[list[dict], bool]:
    """Returns (questions, was_truncated_or_repaired)."""
    try:
        data = json.loads(raw_text)
        return data.get("questions", []), False
    except json.JSONDecodeError:
        return extract_complete_objects(raw_text), True


def render_text_block(questions: list[dict]) -> str:
    blocks = []
    for q in questions:
        lines = [q.get("question_text", "").strip()]
        if q.get("table_markdown"):
            lines.append(q["table_markdown"].strip())
        for s in q.get("statements") or []:
            lines.append(s)
        options = q.get("options") or []
        for idx, opt in enumerate(options):
            line = f"{opt.get('label', chr(65 + idx))}) {opt.get('text', '')}"
            if opt.get("is_correct"):
                line += " ✅"
            lines.append(line)
        expl = q.get("explanation", "").strip()
        if expl and not expl.startswith("Ex:"):
            expl = f"Ex: {expl}"
        lines.append(expl)
        blocks.append("\n".join(lines))
    body = "\n\n".join(blocks)
    return f"```\n{body}\n```"


# --------------------------------------------------------------------------
# 4. Daily token-usage tracking (local, approximate)
# --------------------------------------------------------------------------

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


# --------------------------------------------------------------------------
# 5. Retry helper (handles API outages / transient errors)
# --------------------------------------------------------------------------

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


# --------------------------------------------------------------------------
# 6. Core generation
# --------------------------------------------------------------------------

def generate_quiz(
    cfg: QuizConfig,
    file_path: Optional[str] = None,
    api_key: Optional[str] = None,
    max_output_tokens: int = 8192,
    daily_token_limit: int = 1_000_000,
    status_callback: Optional[StatusCallback] = None,
    retries: int = 3,
) -> dict[str, Any]:
    def notify(msg: str):
        if status_callback:
            status_callback(msg)

    client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
    contents: list[Any] = []

    # -- Upload inside protective retry block --
    if file_path:
        notify(f"📄 Uploading document ({os.path.basename(file_path)})...")
        try:
            uploaded = _with_retries(
                lambda: client.files.upload(file=file_path),
                retries=retries, status_callback=status_callback, label="Document upload",
            )
            contents.append(uploaded)
            notify("✅ Document uploaded.")
        except Exception as exc:
            return {
                "status": "error",
                "error": f"Upload failed: {exc}",
                "config": cfg.__dict__,
                "questions": [],
                "formatted_text": "```\n\n```",
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

    notify(f"🧠 Formatting with Gemini 2.5 Flash-Lite — generating {cfg.num_questions} questions...")

    start = time.time()
    accumulated_text = ""
    finish_reason = None
    usage_metadata = None
    state = {"last_notify": start}

    def run_stream():
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
            if now - state["last_notify"] >= 2.5:  # Throttled progress update
                elapsed = now - start
                approx_tokens = max(len(accumulated_text) // 4, 1)
                rate = approx_tokens / elapsed if elapsed > 0 else 0
                remaining = max(estimated_total_tokens - approx_tokens, 0)
                eta_str = f"~{round(remaining / rate)}s remaining" if rate > 0 else "estimating time..."
                notify(f"⏱ {round(elapsed)}s elapsed | {eta_str} | ~{approx_tokens} tokens generated so far")
                state["last_notify"] = now

    error = None
    try:
        _with_retries(run_stream, retries=retries, status_callback=status_callback, label="Gemini generation")
    except Exception as exc:
        error = str(exc)

    elapsed_total = round(time.time() - start, 2)

    if error:
        notify(f"❌ Gemini API appears unavailable: {error}")
        return {
            "status": "error",
            "error": error,
            "config": cfg.__dict__,
            "generation_time_seconds": elapsed_total,
            "questions": [],
            "formatted_text": "```\n\n```",
        }

    questions, repaired = parse_quiz_json(accumulated_text) if accumulated_text else ([], True)
    truncated = repaired or (finish_reason is not None and "MAX_TOKENS" in finish_reason)
    status = "truncated" if truncated else "complete"

    total_tokens_this_call = (
        usage_metadata.total_token_count if usage_metadata and hasattr(usage_metadata, "total_token_count")
        else max(len(accumulated_text) // 4, 0)
    )
    usage_report = record_usage(total_tokens_this_call, daily_token_limit)

    if truncated:
        notify(f"⚠️ Response was cut off — {len(questions)}/{cfg.num_questions} questions formatted, rest dropped.")
    else:
        notify(f"✅ Done in {elapsed_total}s — {len(questions)}/{cfg.num_questions} questions formatted successfully.")

    notify(
        f"📊 Tokens used this request: {total_tokens_this_call:,}. "
        f"Today's usage: {usage_report['tokens_used_today']:,} / {usage_report['daily_limit']:,} "
        f"({usage_report['percent_used']}%)."
    )

    return {
        "status": status,
        "error": None,
        "model": MODEL,
        "config": cfg.__dict__,
        "requested_questions": cfg.num_questions,
        "returned_questions": len(questions),
        "truncated": truncated,
        "generation_time_seconds": elapsed_total,
        "finish_reason": finish_reason,
        "tokens_used_this_request": total_tokens_this_call,
        "usage_today": usage_report,
        "questions": questions,
        "formatted_text": render_text_block(questions) if questions else "```\n\n```",
    }


# --------------------------------------------------------------------------
# 7. CLI
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gemini 2.5 Flash-Lite MCQ generator")
    parser.add_argument("--file", help="Path to a PDF/image document (omit if using --topic only)")
    parser.add_argument("--topic", default=None, help="Topic name if no document is attached")
    parser.add_argument("--pages", default="All")
    parser.add_argument("--num-questions", type=int, default=5)
    parser.add_argument("--num-options", type=int, default=4)
    parser.add_argument("--difficulty", default="Medium")
    parser.add_argument("--language", default="English")
    parser.add_argument("--qtype", default="Mixed")
    parser.add_argument("--max-output-tokens", type=int, default=8192)
    parser.add_argument("--daily-token-limit", type=int, default=1_000_000)
    parser.add_argument("--out", default=None, help="Write JSON result to this file")
    args = parser.parse_args()

    source = args.topic if args.topic else ("Attached Document" if args.file else "General Knowledge")
    cfg = QuizConfig(
        topic_or_source=source,
        page_range=args.pages,
        num_questions=args.num_questions,
        num_options=args.num_options,
        difficulty=args.difficulty,
        languages=args.language,
        question_types=args.qtype,
    )

    def status_callback(msg: str):
        # Swap this for `await bot.send_message(chat_id, msg)` in your Telegram handler.
        print(msg, file=sys.stderr)

    result = generate_quiz(
        cfg,
        file_path=args.file,
        max_output_tokens=args.max_output_tokens,
        daily_token_limit=args.daily_token_limit,
        status_callback=status_callback,
    )

    out_json = json.dumps(result, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_json)
        print(f"Saved to {args.out}")
    else:
        print(out_json)


if __name__ == "__main__":
    main()
