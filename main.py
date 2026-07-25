"""
╔══════════════════════════════════════════════════════════════════════════╗
║                      QUIZBOT — Main Bot (Pyrogram)                       ║
║                                                                          ║
║  A powerful Telegram quiz bot built with Pyrogram.                       ║
║  Supports quiz creation, analytics, file imports, inline queries,        ║
║  broadcast, assignments, HTML reports, and much more.                    ║
║                                                                          ║
║  Sponsored by  : Qzio — qzio.in                                         ║
║  Developed by  : devgagan — devgagan.in                                  ║
║  License       : MIT                                                     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
import asyncio, aiohttp, html, io, json, logging, os, random, re, string, sys, traceback, fractions, uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import aiofiles
from collections import Counter, defaultdict
import gc, requests
import pymongo
from pymongo.errors import PyMongoError
from sympy.parsing.latex import parse_latex
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient

from pyrogram import Client, filters
from pyrogram.enums import PollType, ChatType
from pyrogram.errors import (
    ChatAdminRequired, FloodWait, InviteHashExpired, InviteHashInvalid,
    UserAlreadyParticipant, UserNotParticipant
)
from pyrogram.types import InlineQueryResultArticle, InlineKeyboardMarkup, InlineKeyboardButton, InputTextMessageContent

from pyrogram.raw.functions.messages import GetPollVotes, GetPollResults
from pyrogram.raw.types import InputPeerChat
from pyrogram.types import (
    Message, User, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    InlineQueryResultArticle, InputTextMessageContent
)
from quiz_generator import generate_quiz
from func import clean_html
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
import binascii
import time
from bson import ObjectId
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

from config import (
    API_ID, API_HASH, BOT_TOKEN, BOT_TOKEN_2,
    MONGO_URI, MONGO_URI_2, MONGO_URIX, DB_NAME,
    OWNER_ID, BOT_GROUP, CHANNEL_ID,
    MASTER_KEY, IV_KEY
)

app = Client("TutorialSCBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=50)

# ── Database connections ──────────────────────────────────────────────────────
client_db = pymongo.MongoClient(MONGO_URI)
db = client_db[DB_NAME]
users_collection     = db["quiz_users"]
questions_collection = db["questions"]
auth_chats_collection = db["auth_chats"]

mongo_client = pymongo.MongoClient(MONGO_URI_2)
mdb = mongo_client["assignment_bot"]
assignments_collection = mdb["assignments"]
submissions_collection = mdb["submissions"]

cl2_db = pymongo.MongoClient(MONGO_URI_2)
db2 = cl2_db[DB_NAME]
uc_2 = db2["quiz_users"]
qc_2 = db2["questions"]
ac_2 = db2["auth_chats"]

BOT_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN_2}"

# ── Constants ─────────────────────────────────────────────────────────────────
PAGE_SIZE = 10

# ── State ─────────────────────────────────────────────────────────────────────
ongoing_edits   = {}
user_quiz_data  = {}
broadcast_active = False
TEMP_ACCESS     = {}

MASTER_KEY_HEX = binascii.hexlify(MASTER_KEY.encode() if isinstance(MASTER_KEY, str) else MASTER_KEY).decode()
IV_HEX         = binascii.hexlify(IV_KEY.encode() if isinstance(IV_KEY, str) else IV_KEY).decode()
MASTER_KEY_B   = binascii.unhexlify(MASTER_KEY_HEX)
IV_B           = binascii.unhexlify(IV_HEX.ljust(32, "0"))[:16]

user_quiz_data = {}
broadcast_active = False 


TEMP_ACCESS = {}


def encrypt_test_id(test_id: str) -> str:
    cipher = AES.new(MASTER_KEY_B, AES.MODE_CBC, IV_B)
    padded_data = pad(test_id.encode(), AES.block_size)
    encrypted = cipher.encrypt(padded_data)
    return base64.urlsafe_b64encode(encrypted).decode()

def decrypt_test_id(encrypted_id: str) -> str:
    padding_needed = 4 - (len(encrypted_id) % 4)
    if padding_needed and padding_needed != 4:
        encrypted_id += "=" * padding_needed

    cipher = AES.new(MASTER_KEY_B, AES.MODE_CBC, IV_B)
    encrypted_data = base64.urlsafe_b64decode(encrypted_id)
    decrypted = unpad(cipher.decrypt(encrypted_data), AES.block_size)
    return decrypted.decode()

FEATURES_TEXT = """> **📢 Features Showcase of Quizbot!** 🚀  

🔹 **Create questions from text** just by providing a ✅ mark to the right options.  
🔹 **Marathon Quiz Mode:** Create unlimited questions for a never-ending challenge.  
🔹 **Convert Polls to Quizzes:** Simply forward polls (e.g., from @TutorialSCBot), and unnecessary elements like `[1/100]` will be auto-removed!  
🔹 **Smart Filtering:** Remove unwanted words (e.g., usernames, links) from forwarded polls.  
🔹 **Skip, Pause & Resume** ongoing quizzes anytime.  
🔹 **Bulk Question Support** via ChatGPT output.  
🔹 **Negative Marking** for accurate scoring.  
🔹 **Edit Existing Quizzes** with ease like shuffle title editing timer adding removing questions and many more.  
🔹 **Quiz Analytics:** View engagement, tracking how many users completed the quiz.  
🔹 **Inline Query Support:** Share quizzes instantly via quiz ID.  
🔹 **Assignment Management:** Track student responses via bot submissions.  
🔹 **View Creator Info** using the quiz ID.  
🔹 **Generate Beautiful HTML Reports** with score counters, plus light/dark theme support.  
🔹 **Auto-Send Group Results:** No need to copy-paste manually—send all results in one click! 
🔹 **Create Sectional Quiz:** You can create different sections with different timing 🥳.
🔹 **Slow/Fast**: Slow or fast ongoing quiz.
🔹 **OCR Update** - Now extract text from PDFs or Photos
🔹 **Comparison** of Result with accuracy, percentile and percentage
🔹 Create Questions from TXT.
🔹 Advance Mechanism with 99.99% uptime.
🔹 Automated link and username removal from Poll's description and questions.
🔹 Auto txt quiz creation from Wikipedia Britannia bbc news and 20+ articles sites.
"""

START_TEXT = """
👋 **Welcome to the Sammy Cares Tutorial Bot!**

I'm here to help you create, manage, and share quizzes with ease.

**📚 Quick Start**
• `/create` — Create a new quiz.
• `/myquizzes` — View all your quizzes.
• `/listquiz` — Browse available quizzes.
• `/edit` — Edit an existing quiz.
• `/stopedit` — Exit editing mode.
• `/transfer` — Transfer a quiz to another user.
• `/assignment` — Access assignment features.
• `/del` — Delete a specific quiz.
• `/delall` — Delete all your quizzes.
• `/cancel` — Cancel the current operation.
• `/features` — See everything the bot can do.
• `/help` — Get detailed help and guidance.

💡 **Tip:** Type `/` in the chat to see the available commands, or use `/help` if you're unsure where to begin.

Happy quizzing! 🎉
"""

def generate_random_id():
    return "GGN" + "".join(random.choices(string.ascii_uppercase + string.digits, k=7))


async def remove_baby(text):
    if not text:
        return text

    text = re.sub(r'[\[\(]\s*Q\.?\s*\d+\s*/\s*\d+\s*[\]\)]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bQ\.?\s*\d+\s*/\s*\d+\)?', '', text, flags=re.IGNORECASE)

    text = re.sub(r'[\[\(]?\s*Q\.?\s*\d+\s*[\]\)]?', '', text, flags=re.IGNORECASE)
    pattern = r"(https?://[^\s]+|t\.me/[^\s]+|@\w+)"
    text = re.sub(pattern, "", text)

    return text.strip()
    

@app.on_message(filters.command("delall") & filters.user(OWNER_ID))
async def delete_all_quizzes(client, message: Message):
    result = questions_collection.delete_many({})
    await message.reply(f"✅ Deleted {result.deleted_count} quiz records from the database.")


async def send_document_http(chat_id: int, file_id: str, caption: str):
    payload = {
        "chat_id": chat_id,
        "document": file_id,
        "caption": caption
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{BOT_API_URL}/sendDocument",
            json=payload
        ) as resp:
            return await resp.json()
            

@app.on_message(filters.command("del") & filters.private)
async def delete_quiz(client, message: Message):
    args = message.text.split()
    if len(args) != 2:
        await message.reply("❌ Please provide a valid Question Set ID. Example: `/del 12345`")
        return

    question_set_id = args[1]
    user_id = message.from_user.id

    quiz = questions_collection.find_one({"question_set_id": question_set_id})

    if not quiz:
        await message.reply("❌ No quiz found with the given Question Set ID.")
        return

    if quiz["creator_id"] != user_id:
        await message.reply("❌ You are not authorized to delete this quiz.")
        return

    questions_collection.delete_one({"question_set_id": question_set_id})
    await message.reply(f"✅ Quiz with Question Set ID `{question_set_id}` has been deleted.")
    

@app.on_message(filters.command("transfer") & filters.user(OWNER_ID))
async def transfer_quizzes(client, m):
    """Transfer all quizzes from one creator to another - Owner only"""
    args = m.text.split()
    
    if len(args) != 3:
        await m.reply(
            "❌ Invalid format!\n\n"
            "**Usage:** `/transfer FROM_ID TO_ID`\n"
            "**Example:** `/transfer 123456789 987654321`"
        )
        return
    
    try:
        from_id = int(args[1])
        to_id = int(args[2])
    except ValueError:
        await m.reply("❌ Both IDs must be valid numbers!")
        return
    
    if from_id == to_id:
        await m.reply("❌ FROM_ID and TO_ID cannot be the same!")
        return
    
    pm = await m.reply(f"🔄 Transferring quizzes from `{from_id}` to `{to_id}`...")
    
    try:

        count = questions_collection.count_documents({"creator_id": from_id})
        
        if count == 0:
            await pm.edit_text(f"❌ No quizzes found for creator ID `{from_id}`")
            return
        

        result = questions_collection.update_many(
            {"creator_id": from_id},
            {"$set": {"creator_id": to_id}}
        )
        
        await pm.edit_text(
            f"✅ **Transfer Complete!**\n\n"
            f"**Transferred:** {result.modified_count} quiz(es)\n"
            f"**From:** `{from_id}`\n"
            f"**To:** `{to_id}`"
        )
        
    except Exception as e:
        await pm.edit_text(f"❌ Transfer failed: {str(e)}")
        print(f"Transfer error: {e}")


# Cancel Command Handler
@app.on_message(filters.command("cancel") & filters.private)
async def cancel_quiz_creation(client, message):
    user_id = message.from_user.id
    

    if user_id in user_quiz_data:

        user_quiz_data.pop(user_id, None)
        await message.reply("❌ Question creation canceled. You can start again by sending a new set of questions.")
    else:
        await message.reply("⚠️ No ongoing question creation to cancel.")


# Custom JSON encoder that handles MongoDB types
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

CACHE_DIR = "quiz_cache"
CACHE_EXPIRY = 600  # 10 minutes in seconds
PAGE_SIZE = 5  # Number of quizzes per page

# Ensure cache directory exists
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_file(user_id):
    """Get the cache file path for a user"""
    return os.path.join(CACHE_DIR, f"user_{user_id}.json")

def save_quiz_cache(user_id, quizzes, search_terms=None):
    """Save user's quiz data to a file with custom JSON encoder"""
    cache_file = get_cache_file(user_id)
    cache_data = {
        "data": quizzes,
        "timestamp": time.time(),
        "search_terms": search_terms  # Store search terms if any
    }
    with open(cache_file, "w") as f:
        json.dump(cache_data, f, cls=MongoJSONEncoder)

def load_quiz_cache(user_id):
    """Load user's quiz data from file if it exists and isn't expired"""
    cache_file = get_cache_file(user_id)
    
    if not os.path.exists(cache_file):
        return None
    
    try:
        with open(cache_file, "r") as f:
            cache_data = json.load(f)
        

        if time.time() - cache_data["timestamp"] > CACHE_EXPIRY:
            os.remove(cache_file)  # Delete expired cache
            return None
            
        return cache_data
    except (json.JSONDecodeError, KeyError, FileNotFoundError):

        if os.path.exists(cache_file):
            os.remove(cache_file)
        return None

def prepare_quiz_for_cache(quiz):
    """Convert MongoDB document to a serializable dictionary"""
    if not isinstance(quiz, dict):
        quiz = dict(quiz)
    

    for key, value in quiz.items():
        if isinstance(value, ObjectId):
            quiz[key] = str(value)
        elif isinstance(value, datetime):
            quiz[key] = value.isoformat()
    
    return quiz

def filter_quizzes_by_search(quizzes, search_terms):
    """Filter quizzes by search terms in quiz name"""
    if not search_terms:
        return quizzes
    
    filtered_quizzes = []
    search_terms_lower = [term.lower() for term in search_terms]
    
    for quiz in quizzes:
        quiz_name = quiz.get('quiz_name', '').lower()
        

        if all(term in quiz_name for term in search_terms_lower):
            filtered_quizzes.append(quiz)
    
    return filtered_quizzes

async def send_quiz_page(client, message, quizzes, page_number, user_id, search_terms=None):
    """Send a page of quizzes to the user"""
    start = page_number * PAGE_SIZE
    end = start + PAGE_SIZE
    current_page_quizzes = quizzes[start:end]

    if not current_page_quizzes:
        await message.edit_text("❌ No quizzes found on this page.")
        return

    quiz_list = "\n".join(
        [
            f"**{start + i + 1}. {quiz.get('quiz_name', 'Unnamed Quiz')}**\n"
            f"    - 🆔 Quiz ID: `{quiz.get('question_set_id', 'N/A')}`\n"
            f"    - 🗄️ Database: `{quiz.get('source_db', 'Unknown')}`\n"
            f"    - 👥 Users: {quiz.get('total_participation', 0)}\n"
            f"    - 🗽 Start: `/start {quiz.get('question_set_id', 'N/A')}`\n"
            f"    - 🥊 Share: `@TutorialSCBot {quiz.get('question_set_id', 'N/A')}`\n"
            f"    - 🖊️ Edit: `/edit {quiz.get('question_set_id', 'N/A')}`\n\n────────────────\n"
            for i, quiz in enumerate(current_page_quizzes)
        ]
    )

    keyboard = []
    if len(quizzes) > PAGE_SIZE:
        buttons = []
        if page_number > 0:
            buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"prev:{page_number}:{user_id}"))
        if end < len(quizzes):
            buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"next:{page_number}:{user_id}"))
        

        total_pages = (len(quizzes) + PAGE_SIZE - 1) // PAGE_SIZE
        buttons.append(InlineKeyboardButton(f"📄 {page_number + 1}/{total_pages}", callback_data="page_info"))
        
        keyboard.append(buttons)
    

    if search_terms:
        keyboard.append([InlineKeyboardButton("🔍 Clear Search", callback_data=f"clear_search:{user_id}")])
    

    keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh:{user_id}"), 
                     InlineKeyboardButton("❌ Close", callback_data=f"close:{user_id}")])

    total_quizzes = len(quizzes)
    qc2_count = len([q for q in quizzes if q.get('source_db') == 'qc_2'])
    qc_count = len([q for q in quizzes if q.get('source_db') == 'question_collection'])
    

    header_text = f"📝 **Your Quizzes (Page {page_number + 1})**\n"
    if search_terms:
        header_text += f"🔍 **Search:** `{' '.join(search_terms)}`\n"
    header_text += f"📊 Total: {total_quizzes} | DB 2: {qc2_count} | DB 1: {qc_count}\n\n"

    await message.edit_text(
        f"{header_text}{quiz_list}",
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
    )

@app.on_message(filters.command("myquizzes") & filters.private)
async def list_user_quizzes(client, message: Message):
    user_id = message.from_user.id

    search_terms = []
    if len(message.command) > 1:

        search_text = ' '.join(message.command[1:])

        search_terms = search_text.split()
    

    if search_terms:
        progress_message = await message.reply(f"🔍 Searching quizzes for: `{' '.join(search_terms)}`...")
    else:
        progress_message = await message.reply("🔍 Fetching all quizzes from both databases...")
    

    quizzes_qc2 = list(qc_2.find({"creator_id": user_id}))
    quizzes_question_collection = list(questions_collection.find({"creator_id": user_id}))
    

    all_quizzes = []
    

    for quiz in quizzes_qc2:
        quiz = prepare_quiz_for_cache(quiz)
        quiz['source_db'] = 'DB 2'
        all_quizzes.append(quiz)
    

    for quiz in quizzes_question_collection:
        quiz = prepare_quiz_for_cache(quiz)
        quiz['source_db'] = 'DB 1'
        all_quizzes.append(quiz)
    

    if search_terms:
        all_quizzes = filter_quizzes_by_search(all_quizzes, search_terms)
    
    if not all_quizzes:
        if search_terms:
            await progress_message.edit_text(f"❌ No quizzes found containing: `{' '.join(search_terms)}`")
        else:
            await progress_message.edit_text("❌ You haven't created any quizzes in either database.")
        return
    

    save_quiz_cache(user_id, all_quizzes, search_terms)
    

    await send_quiz_page(client, progress_message, all_quizzes, 0, user_id, search_terms)

@app.on_callback_query(filters.regex("^(prev|next|refresh|clear_search|close):"))
async def handle_quiz_pagination(client, callback_query: CallbackQuery):
    data_parts = callback_query.data.split(":")
    action = data_parts[0]
    user_id = int(data_parts[-1])  # user_id is always the last part
    
    if action == "close":
        await callback_query.message.delete()
        await callback_query.answer("Closed!")
        return
        
    elif action == "refresh":

        cache_file = get_cache_file(user_id)
        if os.path.exists(cache_file):
            os.remove(cache_file)
        

        fake_message = callback_query.message
        fake_message.from_user.id = user_id
        fake_message.command = ["myquizzes"]
        
        await list_user_quizzes(client, fake_message)
        await callback_query.message.delete()
        await callback_query.answer("✅ Quizzes refreshed!")
        return
        
    elif action == "clear_search":

        cache_file = get_cache_file(user_id)
        if os.path.exists(cache_file):
            os.remove(cache_file)
        

        fake_message = callback_query.message
        fake_message.from_user.id = user_id
        fake_message.command = ["myquizzes"]
        
        await list_user_quizzes(client, fake_message)
        await callback_query.message.delete()
        await callback_query.answer("✅ Search cleared!")
        return
    

    page_number = int(data_parts[1])
    

    cache_data = load_quiz_cache(user_id)
    if not cache_data:
        await callback_query.answer("❌ Quiz data expired. Please run /myquizzes again.", show_alert=True)
        return
    
    quizzes = cache_data["data"]
    search_terms = cache_data.get("search_terms")
    
    if not quizzes:
        await callback_query.answer("No quizzes available.", show_alert=True)
        return

    new_page_number = page_number - 1 if action == "prev" else page_number + 1
    await send_quiz_page(client, callback_query.message, quizzes, new_page_number, user_id, search_terms)
    await callback_query.answer()
    
# Inline query handler for quizzes and assignments
@app.on_inline_query()
async def handle_inline_query(client, inline_query):
    query = inline_query.query.strip()

    if not query:
        return

    if not query.startswith("ass_"):

        quiz_data = questions_collection.find_one({"question_set_id": query})
        if not quiz_data:
            quiz_data = qc_2.find_one({"question_set_id": query})
        
        if not quiz_data:

            await inline_query.answer(
                results=[],
                switch_pm_text="No quiz found for this ID",
                switch_pm_parameter="start"
            )
            return

        quiz_name = quiz_data["quiz_name"]
        question_count = len(quiz_data["questions"])
        timer = quiz_data["timer"]
        nmark = quiz_data.get("negative_marking", 0)
        start_deep_link = f"https://t.me/NewSamghibili_bot?start={query}"
        sections = quiz_data.get("sections", [])  
        
        message_text = (
            f"**💳 Quiz Name:** {quiz_name}\n"
            f"**#️⃣ Questions:** {question_count}\n"
            f"**⏰ Timer:** {timer} seconds\n"
            f"**🆔 Quiz ID:** `{query}`\n"
            f"**🏴‍☠️ -ve Marking:** `{nmark}`\n"
        )
        if sections:
            message_text += "\n\n> **📂 Sections:**"
            for i, section in enumerate(sections, start=1):
                section_name = section["name"]
                start_idx, end_idx = section["question_range"]
                section_timer = section.get("timer", "Not specified")
                message_text += (
                    f"\n\n**Section {i}:** {section_name}\n"
                    f"  - **Questions:** {start_idx} to {end_idx}\n"
                    f"  - **Timer:** {section_timer} seconds"
                )
                

        result = InlineQueryResultArticle(
            id=query,
            title=f"Quiz: {quiz_name}",
            description=f"{question_count} questions | Timer: {timer} seconds",
            input_message_content=InputTextMessageContent(
                message_text=message_text,
                disable_web_page_preview=True
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Start Quiz Now", url=f"https://t.me/NewSamghibili_bot?start={query}")],
                [InlineKeyboardButton("🚀 Start in Group", url=f"https://t.me/NewSamghibili_bot?startgroup={query}")],
                [InlineKeyboardButton("🔗 Share Quiz", switch_inline_query=query)],
            ])
        )

        await inline_query.answer([result], cache_time=0)

    else:

        assignment_id = query.split('_')[1]  # Extract assignment ID from query
        assignment = assignments_collection.find_one({"assignment_id": assignment_id})

        if assignment:

            assignment_text = f"""
> 📚 **Assignment Details** 📚

🆔 **Assignment ID:** `{assignment_id}`
👨‍🏫 **Creator:** {assignment['creator_name']}
📅 **Date Created:** {assignment['created_date']}
"""

            results = [
                InlineQueryResultArticle(
                    title=f"Assignment {assignment_id}",
                    input_message_content=InputTextMessageContent(assignment_text),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("DO Assignment", callback_data=f"do_{assignment_id}")],
                        [InlineKeyboardButton("Share Assignment", switch_inline_query=query)]
                    ])
                )
            ]

            await inline_query.answer(results=results)
        else:

            await inline_query.answer(
                results=[],
                switch_pm_text="Assignment not found",
                switch_pm_parameter="error"
            )

user_create_limits = {}

@app.on_message(filters.command("create") & filters.private)
async def create_quiz(client, message: Message):
    user_id = message.from_user.id
    now = datetime.now()
    if user_id in ongoing_edits:
        del ongoing_edits[user_id]

    if user_id not in user_create_limits:
        user_create_limits[user_id] = {"count": 0, "last_used": None, "warned": False}

    data = user_create_limits[user_id]

    if data["last_used"] is None or data["last_used"].date() != now.date():
        data["count"] = 0
        data["last_used"] = None
        data["warned"] = False

    if data["count"] >= 50:
        if not data["warned"]:  # Warn once per day
            await message.reply("⚠️ You can only use /create **50 times per day**. Try again tomorrow.")
            data["warned"] = True
        return  # Ignore silently

    if data["last_used"] and now - data["last_used"] < timedelta(minutes=3):
        if not data["warned"]:  # Warn once until cooldown ends
            await message.reply("⚠️ Please wait **3 minutes** before using /create again.")
            data["warned"] = True
        return  # Ignore silently

    data["count"] += 1
    data["last_used"] = now
    data["warned"] = False  # Reset warning so next time they get warned again if they break rules

    if user_id in user_quiz_data:
        await message.reply("❌ You're already creating a quiz. Finish it first by typing /done, or /cancel to cancel.")
        return

    await message.reply("✅ **Send the quiz name first.**")
    user_quiz_data[user_id] = {"questions": [], "timer": None, "quiz_name": None, "awaiting_name": True, "no_of_questions": None, "awaiting_no_of_questions": False}
    

@app.on_message(filters.command("done") & filters.private)
async def finish_quiz_creation(client, message: Message):
    user_id = message.from_user.id

    if user_id not in user_quiz_data:
        await message.reply("❌ You haven't started creating a quiz. Use /create first.")
        return

    total_questions = len(user_quiz_data[user_id]["questions"])
    
    if total_questions < 10:
        await message.reply(f"❌ You need at least **10 questions** to finish the quiz.\nCurrently, you have **{total_questions}** questions.")
        return

    if total_questions > 250 and user_id != OWNER_ID:
        await message.reply(f"❌ You cant create more than 250 questions per quiz\nCurrently, you have **{total_questions}** questions.")
        return

    await message.reply("Do you want section in your quiz? send yes/no")
    user_quiz_data[user_id]["awaiting_section_choice"] = True

@app.on_message(filters.command("help"))
async def help_command(client, message):
    help_text = (
        "Hey, welcome to help!\n\n"
        "> 📌 **Quiz Commands:**\n"
        "/create - Start creating a quiz\n"
        "/myquizzes - List your quizzes\n"
        "/stop - Stop a running quiz in a group\n"
        "/cancel - Cancel the quiz creation\n"
        "/done - Finish quiz creation\n"
        "/edit - Edit questions\n"
        "/del <quizid> - Remove a quiz from the database\n\n"
    )
    await message.reply(help_text, disable_web_page_preview=True)

@app.on_message(filters.command("gcast") & filters.user(OWNER_ID))
async def broadcast(client, message: Message):
    global broadcast_active
    if not message.reply_to_message:
        await message.reply("Please reply to the message you want to broadcast.")
        return

    if broadcast_active:
        await message.reply("⚠️ A broadcast is already in progress! Wait until it finishes or use /stopcast.")
        return

    broadcast_active = True  # Set flag to active
    broadcast_message = message.reply_to_message
    user_list = list(users_collection.find())

    total_users = len(user_list)
    sent_count = 0
    failed_count = 0

    progress_message = await message.reply(f"📤 Broadcast started: 0/{total_users} users sent")

    for index, user in enumerate(user_list):
        if not broadcast_active:
            await progress_message.edit_text(f"❌ Broadcast stopped at {sent_count}/{total_users} users.")
            return  # Stop the broadcast if canceled

        try:

            if broadcast_message.text:
                await client.send_message(
                    chat_id=user["chat_id"],
                    text=broadcast_message.text,
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            elif broadcast_message.photo:
                await client.send_photo(
                    chat_id=user["chat_id"],
                    photo=broadcast_message.photo.file_id,
                    caption=broadcast_message.caption if broadcast_message.caption else "",
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            elif broadcast_message.video:
                await client.send_video(
                    chat_id=user["chat_id"],
                    video=broadcast_message.video.file_id,
                    caption=broadcast_message.caption if broadcast_message.caption else "",
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            elif broadcast_message.document:
                await client.send_document(
                    chat_id=user["chat_id"],
                    document=broadcast_message.document.file_id,
                    caption=broadcast_message.caption if broadcast_message.caption else "",
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            elif broadcast_message.audio:
                await client.send_audio(
                    chat_id=user["chat_id"],
                    audio=broadcast_message.audio.file_id,
                    caption=broadcast_message.caption if broadcast_message.caption else "",
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            elif broadcast_message.voice:
                await client.send_voice(
                    chat_id=user["chat_id"],
                    voice=broadcast_message.voice.file_id,
                    caption=broadcast_message.caption if broadcast_message.caption else "",
                    reply_markup=broadcast_message.reply_markup if broadcast_message.reply_markup else None
                )

            else:
                await client.send_message(
                    chat_id=user["chat_id"],
                    text="📢 Broadcast Message (Unsupported format)",
                )

            sent_count += 1

        except Exception:
            failed_count += 1

        if (index + 1) % 100 == 0 or (index + 1) == total_users:
            await progress_message.edit_text(
                f"📤 Broadcast Progress: {sent_count}/{total_users} users sent"
            )
            await asyncio.sleep(10)  # Sleep for 10 seconds after 100 messages

    
    broadcast_active = False  # Reset flag after completion
    await progress_message.edit_text(
        f"✅ Broadcast completed: {sent_count}/{total_users} users successfully sent\n❌ Failed: {failed_count} users"
    )
@app.on_message(filters.command("stopcast") & filters.user(OWNER_ID))  # Restrict stop command to the owner
async def stop_broadcast(client, message: Message):
    global broadcast_active
    if not broadcast_active:
        await message.reply("⚠️ No active broadcast to stop.")
        return

    broadcast_active = False  # Set flag to stop the ongoing broadcast
    await message.reply("✅ Broadcast has been stopped.")
    

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_quiz(client, message):

    k = await message.reply("Fetching bot statistics...")
    total_users = users_collection.count_documents({})

    total_quizzes = questions_collection.count_documents({})

    quizzes = questions_collection.find()
    removed_count = 0
    free_quizzes = 0
    for quiz in quizzes:
        question_set_id = quiz["question_set_id"]
        total_questions = len(quiz.get("questions", []))
        
        free_quizzes += 1

        if total_questions < 10:

            questions_collection.delete_one({"question_set_id": question_set_id})
            removed_count += 1

    await k.edit(
        f"> 📊 **Bot Statistics:**\n\n"
        f"👥 **Total Registered Users:** `{total_users}`\n"
        f"📚 **Total Quizzes Created:** `{total_quizzes}`\n"
        f"🎉 **Quizzes:** `{free_quizzes}`\n\n"
        f"**__Powered by Zealina__**"
    )
    

@app.on_message(filters.command("remove") & filters.private)
async def handle_remove_command(client, message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    if len(message.command) < 2:
        await message.reply("❌ Please provide words or a sentence to add to the remove list.\n\nUsage: `/remove WORD or SENTENCE`")
        return

    text_to_remove = " ".join(message.command[1:]).strip().lower()  

    words_to_remove = text_to_remove.split()  # Splits sentence into individual words

    users_collection.update_one(
        {"chat_id": chat_id},
        {"$addToSet": {"remove_words": {"$each": words_to_remove}}},  # Add each word separately
        upsert=True
    )

    await message.reply(f"✅ Words `{', '.join(words_to_remove)}` have been added to your remove list.")
    

def filter_words(text, remove_words):
    if not text:
        return text

    text = re.sub(r'\[\s*\d+\s*/\s*\d+\s*\]', '', text).strip()

    if remove_words:
        for word in remove_words:
            pattern = r'\b' + re.escape(word) + r'\b'  # Match whole words
            text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()  # Case-insensitive replacement

    return re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    

async def read_questions_from_file(file_path: str, user_id: str, remove_words: Optional[List[str]] = None, 
                                   app: Optional[Client] = None, log_group_id: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
    if not (file_path.endswith('.txt') or file_path.endswith('.json')):
        return None, "Invalid file format. Only .txt and .json files are supported."

    try:
        if file_path.endswith('.json'):
            return await _process_json_file(file_path, user_id, remove_words, app, log_group_id)
        else:

            return await _process_txt_file(file_path, user_id, remove_words)
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None, f"Error processing file: {str(e)}"

async def _download_and_upload_telegram_file(app: Client, telegram_file_id: str, log_group_id: str) -> Optional[str]:
    """
    Download file from Telegram using file_id with Pyrogram, upload to log group, and return new file_id.
    """
    try:
        print(f"Downloading file with ID: {telegram_file_id[:50]}...")
        

        download_path = await app.download_media(
            telegram_file_id,
            file_name=f"temp_{telegram_file_id[:10]}.jpg"
        )
        
        if not download_path:
            print("Failed to download file")
            return None
        
        print(f"File downloaded to: {download_path}")
        

        try:
            message = await app.send_photo(
                chat_id=log_group_id,
                photo=download_path,
                caption=f"Uploaded from file_id: {telegram_file_id[:20]}..."
            )
            

            if message.photo:

                new_file_id = message.photo.file_id
                print(f"File uploaded successfully. New file_id: {new_file_id[:50]}...")
            else:
                print("No photo in uploaded message")
                new_file_id = None
            
        except Exception as upload_error:
            print(f"Error uploading to log group: {upload_error}")
            new_file_id = None
        

        try:
            os.remove(download_path)
            print("Temporary file cleaned up")
        except:
            pass
        
        return new_file_id
        
    except Exception as e:
        print(f"Error in _download_and_upload_telegram_file: {str(e)}")

        try:
            if 'download_path' in locals() and os.path.exists(download_path):
                os.remove(download_path)
        except:
            pass
        return None

async def _process_text_lengths(question_text: str, options: List[str], reply_text: str = None) -> Tuple[str, List[str], str]:
    """
    Process text lengths according to requirements:
    - If question > 200 chars: truncate question to 100 chars + "...", add full question to reply_text
    - If any option > 100 chars: truncate all options to 50 chars + "...", add full question and all options to reply_text
    """
    original_question = question_text
    original_options = options.copy()
    

    if reply_text is None:
        reply_text = ""
    elif reply_text.strip():
        reply_text = reply_text.strip()
    

    any_long_option = any(len(opt) > 100 for opt in options)
    

    needs_separator = bool(reply_text)
    

    if len(question_text) > 200:

        truncated_question = question_text[:100].rstrip() + "..."
        

        if needs_separator:
            reply_text += "\n\n"
        reply_text += f"Full Question:\n{question_text}"
        needs_separator = True
        
        question_text = truncated_question
    

    if any_long_option:

        truncated_options = []
        for opt in options:
            if len(opt) > 100:
                truncated_opt = opt[:50].rstrip() + "..."
            else:
                truncated_opt = opt[:50].rstrip() + "..." if len(opt) > 50 else opt
            truncated_options.append(truncated_opt)
        
        options = truncated_options
        

        if not reply_text or "Full Question:" not in reply_text:
            if needs_separator:
                reply_text += "\n\n"
            reply_text += f"Full Question:\n{original_question}"
            needs_separator = True
        
        reply_text += "\n\nFull Options:"
        for i, opt in enumerate(original_options):
            reply_text += f"\n{chr(97 + i)}) {opt}"
    
    return question_text, options, reply_text

def decrypt_quiz_file(file_path, auth_key="brandnewday2"):
    """
    Decrypts an encrypted quiz file and saves it back to the same path
    
    Args:
        file_path: Path to the encrypted JSON file
        auth_key: Authorization key (default: codedbytedance2)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:

        with open(file_path, 'r', encoding='utf-8') as f:
            encrypted_b64 = f.read().strip()
        

        encrypted = base64.b64decode(encrypted_b64)
        key_bytes = auth_key.encode('utf-8')
        

        decrypted = bytearray()
        for i, byte in enumerate(encrypted):
            decrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        

        json_str = decrypted.decode('utf-8')
        

        quiz_data = json.loads(json_str)
        

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(quiz_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Successfully decrypted: {file_path}")
        return True
        
    except Exception as e:
        print(f"❌ Decryption failed: {e}")
        return False

async def _process_json_file(file_path: str, user_id: str, remove_words: Optional[List[str]] = None,
                           app: Optional[Client] = None, log_group_id: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
    """Process JSON file format with Pyrogram file handling."""

    try:
        
        try:
            decrypt_quiz_file(file_path)
        except Exception as e:
            print(e)
            pass

        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        

        if not isinstance(data, dict) or "questions" not in data:
            return None, "Invalid JSON format. Expected object with 'questions' array."
        
        questions = data["questions"]
        if not isinstance(questions, list):
            return None, "Invalid JSON format. 'questions' should be an array."
        

        if user_id not in user_quiz_data:
            user_quiz_data[user_id] = {"questions": []}
        elif "questions" not in user_quiz_data[user_id]:
            user_quiz_data[user_id]["questions"] = []
        
        processed_count = 0
        
        for q_idx, question_data in enumerate(questions):
            try:

                required_fields = ["question_text", "options", "correct_option_id"]
                for field in required_fields:
                    if field not in question_data:
                        print(f"Question {q_idx + 1} missing required field: {field}")
                        continue
                

                question_text = question_data["question_text"]
                if remove_words:
                    question_text = filter_words(question_text, remove_words)
                question_text = await remove_baby(question_text)
                

                options_data = question_data["options"]
                if not isinstance(options_data, list) or len(options_data) < 2:
                    print(f"Question {q_idx + 1} has invalid options")
                    continue
                
                options = []
                option_id_map = {}
                valid_option_ids = []
                
                for opt_idx, option in enumerate(options_data):
                    if not isinstance(option, dict) or "id" not in option or "text" not in option:
                        print(f"Question {q_idx + 1} option {opt_idx} is invalid")
                        continue
                    
                    option_id = option["id"]
                    option_text = option["text"]
                    
                    if remove_words:
                        option_text = filter_words(option_text, remove_words)
                    option_text = await remove_baby(option_text)
                    
                    if not option_text:
                        print(f"Question {q_idx + 1} option {option_id} has empty text")
                        continue
                    
                    options.append(option_text)
                    option_id_map[option_id] = opt_idx
                    valid_option_ids.append(option_id)
                
                if len(options) < 2:
                    print(f"Question {q_idx + 1} has insufficient valid options")
                    continue
                

                correct_option_id = question_data["correct_option_id"]
                if correct_option_id not in option_id_map:
                    print(f"Question {q_idx + 1} has invalid correct_option_id: {correct_option_id}")
                    continue
                

                correct_option_index = option_id_map[correct_option_id]
                

                explanation = None
                if "explanation" in question_data and question_data["explanation"]:
                    explanation = question_data["explanation"]
                    if remove_words:
                        explanation = filter_words(explanation, remove_words)
                    explanation = await remove_baby(explanation)
                

                reply_text = None
                if "reference_text" in question_data and question_data["reference_text"]:
                    reply_text = question_data["reference_text"]
                    if remove_words:
                        reply_text = filter_words(reply_text, remove_words)
                    reply_text = await remove_baby(reply_text)
                

                telegram_file_id = None
                new_file_id = None
                
                if "image_url" in question_data and question_data["image_url"]:

                    telegram_file_id = question_data["image_url"]
                    

                    if app and log_group_id:
                        new_file_id = await _download_and_upload_telegram_file(app, telegram_file_id, log_group_id)
                        if not new_file_id:
                            print(f"Warning: Failed to upload image for question {q_idx + 1}")
                

                processed_question, processed_options, processed_reply_text = await _process_text_lengths(
                    question_text, options, reply_text
                )
                

                user_quiz_data[user_id]["questions"].append({
                    "question": processed_question,
                    "options": processed_options,
                    "correct_option_id": correct_option_index,  # Convert to 0-based index
                    "explanation": explanation,
                    "reply_text": processed_reply_text,
                    "file_id": new_file_id,  # Use the new file_id from log group
                })
                
                processed_count += 1
                print(f"Added question {processed_count}: {question_data.get('id', 'No ID')}")
                print(f"  Question: {processed_question[:50]}...")
                print(f"  Options: {[opt[:30] + '...' if len(opt) > 30 else opt for opt in processed_options]}")
                if new_file_id:
                    print(f"  File uploaded, new file_id: {new_file_id[:20]}...")
                
            except Exception as e:
                print(f"Error processing question {q_idx + 1}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"Successfully processed {processed_count} questions from JSON")
        os.remove(file_path)
        return processed_count, None
        
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON format: {str(e)}"
    except Exception as e:
        print(f"Error processing JSON file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, f"Error processing JSON file: {str(e)}"

async def _process_txt_file(file_path: str, user_id: str, remove_words: Optional[List[str]] = None) -> Tuple[Optional[int], Optional[str]]:
    """Process TXT file format - KEEPING ORIGINAL LOGIC INTACT."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            file_content = file.read()

        ggn_tag_pattern = r'<ggn>(.*?)</ggn>'
        ggn_blocks = []
        
        def replace_ggn(match):
            ggn_content = match.group(1)
            placeholder = f"GGN_PLACEHOLDER_{len(ggn_blocks)}"
            ggn_blocks.append(ggn_content)
            return f"RT: <ggn>{placeholder}</ggn>"
        

        protected_content = re.sub(ggn_tag_pattern, replace_ggn, file_content, flags=re.DOTALL)
        

        if user_id not in user_quiz_data:
            user_quiz_data[user_id] = {"questions": []}
        elif "questions" not in user_quiz_data[user_id]:
            user_quiz_data[user_id]["questions"] = []

        questions_blocks = protected_content.strip().split("\n\n")
        processed_count = 0
        

        print(f"Found {len(questions_blocks)} question blocks in file")

        for block_idx, block in enumerate(questions_blocks):
            try:
                print(f"Processing block {block_idx+1}/{len(questions_blocks)}")
                
                lines = block.strip().split("\n")
                if not lines:
                    print(f"Block {block_idx+1} is empty, skipping")
                    continue

                question = lines[0].strip()
                options = []
                correct_option_id = None
                explanation = None
                reply_text = None
                file_id = None  # This is already the Telegram file ID in TXT format

                if remove_words:
                    question = filter_words(question, remove_words)
                question = await remove_baby(question)
                
                print(f"Question: {question}")

                i = 1
                while i < len(lines):
                    line = lines[i].strip()
                    print(f"Processing line: {line}")

                    if line.startswith("Ex:"):
                        explanation = line[3:].strip()
                        if remove_words:
                            explanation = filter_words(explanation, remove_words)
                        explanation = await remove_baby(explanation)
                        i += 1
                        continue

                    if line.startswith("RT:"):
                        rt_content = line[3:].strip()
                        

                        ggn_placeholder_match = re.search(r'<ggn>GGN_PLACEHOLDER_(\d+)</ggn>', rt_content)
                        if ggn_placeholder_match:

                            placeholder_index = int(ggn_placeholder_match.group(1))
                            if placeholder_index < len(ggn_blocks):

                                reply_text = ggn_blocks[placeholder_index]
                        else:

                            reply_text = rt_content
                        
                        if remove_words:
                            reply_text = filter_words(reply_text, remove_words)
                        reply_text = await remove_baby(reply_text)
                        i += 1
                        continue

                    if line.startswith("ID:"):
                        file_id = line[3:].strip()
                        file_id = await remove_baby(file_id)
                        i += 1
                        continue

                    if i < len(lines):
                        clean_option = line.strip()
                        

                        if "✅" in clean_option:
                            correct_option_id = len(options)
                            clean_option = clean_option.replace("✅", "").strip()
                        
                        if remove_words:
                            clean_option = filter_words(clean_option, remove_words)
                        clean_option = await remove_baby(clean_option)
                        

                        if clean_option:
                            options.append(clean_option)
                    
                    i += 1

                processed_question, processed_options, processed_reply_text = await _process_text_lengths(
                    question, options, reply_text
                )

                if not processed_question or len(processed_options) < 2 or correct_option_id is None:
                    print(f"Skipping invalid question: {processed_question}")
                    print(f"Options: {processed_options}, Correct ID: {correct_option_id}")
                    continue

                user_quiz_data[user_id]["questions"].append({
                    "question": processed_question,
                    "options": processed_options,
                    "correct_option_id": correct_option_id,
                    "explanation": explanation,
                    "reply_text": processed_reply_text,
                    "file_id": file_id,  # Original Telegram file ID from TXT
                })
                processed_count += 1
                print(f"Added question {processed_count}")
                print(f"  Processed question: {processed_question[:50]}...")
                print(f"  Options: {[opt[:30] + '...' if len(opt) > 30 else opt for opt in processed_options]}")
                if file_id:
                    print(f"  File ID: {file_id[:20]}...")
                
            except Exception as e:
                print(f"Error processing block {block_idx+1}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue

        print(f"Successfully processed {processed_count} questions")
        os.remove(file_path)
        return processed_count, None

    except Exception as e:
        print(f"Error processing TXT file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, f"Error processing TXT file: {str(e)}"

@app.on_message(filters.command("clearlist") & filters.private)
async def handle_clearlist_command(client, message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    users_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"remove_words": []}}  # Set the remove_words list to an empty array
    )

    await message.reply("✅ Your remove list has been cleared.")
    
    
@app.on_message(filters.document & filters.private)
async def handle_document_messages(client, message: Message):

    import os

    user_id = message.from_user.id
    chat_id = message.chat.id
    
    try:

        if user_id not in user_quiz_data:

            return
        

        allowed_types = [
                "text/plain",
                "application/json",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/pdf"
            ]
        if message.document.mime_type not in allowed_types:
            await message.reply("Only (.txt), (.docx) and (.pdf) files are supported for quiz questions.")
            return
        

        status_msg = await message.reply("Downloading and processing file...")
        

        print(f"Downloading file: {message.document.file_name}")
        file_path = await message.download()
        print(f"File downloaded to: {file_path}")

        file_name, file_extension = os.path.splitext(file_path)
        if file_extension != ".txt":
            print("getting reponse...")
            response = await generate_quiz(file_path=file_path, tgm_message=status_msg, no_of_questions=user_quiz_data[user_id]["no_of_questions"])
            print("response gotten")
            if response["status"] != "complete":
                status_msg = await status_msg.edit_text(f"❌ Error: {response['error']}")
            with open(f"{file_name}.txt", "w") as fp:
                fp.write(response["formatted_text"])
                if os.path.exists(file_path):
                    os.remove(file_path)
            file_path = f"{file_name}.txt"

        user_data = users_collection.find_one({"chat_id": chat_id})
        remove_words = user_data.get("remove_words", []) if user_data else []
        

        print("Starting file processing...")
        processed_count, error = await read_questions_from_file(file_path, user_id, remove_words)
        print("File processing complete.")
        

        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted temporary file: {file_path}")
        

        if error:
            await status_msg.edit_text(f"❌ Error: {error}")
        else:
            total = len(user_quiz_data[user_id]["questions"])
            await status_msg.edit_text(
                f"✅ {processed_count} questions processed from file! Total questions: {total}\n\n"
                f"Send the next question set or poll or type /done when finished or /cancel to cancel."
            )
    
    except Exception as e:
        print(f"Exception in document handler: {str(e)}")
        await message.reply(f"❌ An error occurred while processing the file: {str(e)}")

@app.on_message(filters.command("ban") & filters.private & filters.user(OWNER_ID))
async def ban_quiz(client, message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Please provide a valid **Quiz ID**.\nExample: `/ban 12345`")
        return

    quiz_id = args[1]
    quiz = questions_collection.find_one({"question_set_id": quiz_id})

    if not quiz:
        await message.reply("❌ No quiz found with this ID.")
        return

    creator_id = quiz.get("creator_id")

    if not creator_id or not CHANNEL_ID:
        await message.reply("❌ Could not find the creator or channel info for this quiz.")
        return

    try:
        await client.ban_chat_member(CHANNEL_ID, creator_id)
        await message.reply(f"✅ Banned user `{creator_id}` from bot.")
    except Exception as e:
        await message.reply(f"❌ Failed to remove user from the channel: {e}")

    deleted_quizzes = questions_collection.delete_many({"creator_id": creator_id})
    
    await message.reply(f"🗑️ Deleted `{deleted_quizzes.deleted_count}` quizzes created by `{creator_id}`.")

@app.on_message(filters.command("features"))
async def features_command(client, message):
    await message.reply_text(FEATURES_TEXT, disable_web_page_preview=True)

@app.on_message(filters.command("start"))
async def features_command(client, message):
        await message.reply_text(START_TEXT, disable_web_page_preview=True)

@app.on_message(filters.command("listquiz") & filters.chat(OWNER_ID))
async def list_quizzes(client, message):
    quizzes = list(questions_collection.find())

    if not quizzes:
        await message.reply("❌ No quizzes found.")
        return

    for index, quiz in enumerate(quizzes):
        quiz_name = quiz.get("quiz_name", "Unnamed Quiz")
        question_set_id = quiz.get("question_set_id")
        num_questions = len(quiz.get("questions", []))
        timer = quiz.get("timer", "Not specified")
        negative_marking = quiz.get("negative_marking", 0)
        creator_id = quiz.get("creator_id", "Unknown")
        quiz_name = re.sub(r"(https?://\S+|@\S+|/[\w\d_-]+)", "", quiz_name)

        start_deep_link = f"https://t.me/NewSamghibili_bot?start={question_set_id}"
        group_start_deep_link = f"https://t.me/NewSamghibili_bot?startgroup={question_set_id}"

        quiz_text = (
            f"📌 **Quiz {index + 1}\n\n"
            f"**💳 Name:** `{quiz_name}`\n"
            f"**#️⃣ Questions:** `{num_questions}`\n"
            f"**⏰ Timer:** `{timer} seconds`\n"
            f"**🆔 Quiz ID:** `{question_set_id}`\n"
            f"**🏴‍☠️ -ve Marking:** `{negative_marking:.2f}`\n"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎯 Start Now", url=start_deep_link)],
            [InlineKeyboardButton("🚀 Start in Group", url=group_start_deep_link)]
        ])

        await message.reply(quiz_text, reply_markup=keyboard)
        await asyncio.sleep(3)

        
@app.on_message(filters.command("info") & filters.private)
async def info_quiz(client, message):
    args = message.text.split()
    if len(args) < 2:
        await message.reply("❌ Please provide a valid **Quiz ID**.\nExample: `/info 12345`")
        return

    quiz_id = args[1]
    quiz = questions_collection.find_one({"question_set_id": quiz_id})

    if not quiz:
        await message.reply("❌ No quiz found with this ID.")
        return

    creator_id = quiz["creator_id"]
    creator = await client.get_users(creator_id)
    creator_name = creator.first_name

    await message.reply(f"👨‍🏫 **Creator Name:** {creator_name} his id `{creator_id}`")

@app.on_message(filters.command("assignment") & filters.private)
async def create_assignment(client, message):
    creator_id = message.from_user.id
    creator_name = message.from_user.first_name  # Get creator's name
    

    if message.reply_to_message and message.reply_to_message.document:
        doc_message = message.reply_to_message
    elif message.document:
        doc_message = message
    else:
        await message.reply_text("Please reply to a document or send a document with the /assignment command.")
        return
    

    forwarded = await doc_message.forward(BOT_GROUP)
    file_id = forwarded.document.file_id

    assignment_id = generate_random_id()

    created_date = datetime.now().strftime("%d %B %Y")  # Example: 18 February 2025

    assignment_data = {
        "assignment_id": assignment_id,
        "creator_id": creator_id,
        "file_id": file_id,
        "text": message.caption or "",
        "created_date": created_date,
        "creator_name": creator_name
    }
    assignments_collection.insert_one(assignment_data)

    assignment_text = f"""
> 📚 **New Assignment Posted** 📚

🆔 **Assignment ID:** `{assignment_id}`
👨‍🏫 **Creator:** {creator_name}
📅 **Date Created:** {created_date}
"""

    await message.reply_text(
        f"Assignment Created Successfully! Assignment ID: `{assignment_id}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📁 Share Assignment", switch_inline_query=f"ass_{assignment_id}")]
        ])
    )

def generate_random_id(length=7):

    characters = string.ascii_letters + string.digits  # A-Z, a-z, 0-9
    

    random_id = ''.join(random.choices(characters, k=length))
    
    return random_id

# Callback handler for "DO Assignment" button
@app.on_callback_query(filters.regex(r"do_([\w]+)"))
async def do_assignment(client, callback_query):
    assignment_id = callback_query.data.split("_")[1]
    assignment = assignments_collection.find_one({"assignment_id": assignment_id})

    if assignment:

        student_id = callback_query.from_user.id  # Get student ID
        await client.send_document(
            chat_id=student_id,  # Send the assignment directly to the student
            document=assignment["file_id"],
            caption=f"Assignment ID: `{assignment_id}`\n\n{assignment['text']}",
        )

        await callback_query.answer("Assignment sent to you!", show_alert=True)
    else:
        await callback_query.answer("Assignment not found.", show_alert=True)

@app.on_message(filters.command("submit") & filters.private)
async def submit_assignment(client, message):
    if len(message.command) < 2:
        await message.reply_text("Usage: Reply to a document with `/submit ASSIGNMENT_ID`")
        return

    assignment_id = message.command[1]
    assignment = assignments_collection.find_one({"assignment_id": assignment_id})

    if not assignment:
        await message.reply_text("Invalid Assignment ID.")
        return

    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Please reply to a document with `/submit ASSIGNMENT_ID`.")
        return

    document = message.reply_to_message.document
    student_id = message.from_user.id  # Get student ID
    student_name = message.from_user.first_name  # Get student's first name
    creator_id = assignment["creator_id"]  # Get creator's ID

    existing_submission = submissions_collection.find_one({
        "assignment_id": assignment_id,
        "submitted_by": student_id
    })

    if existing_submission:
        await message.reply_text("You have already submitted this assignment.")
        return

    await client.send_document(
        chat_id=creator_id,
        document=document.file_id,
        caption=f"🔖 **Assignment ID:** {assignment_id}\n🆔 Student ID: {student_id}\n👨‍🎓 Student Name: {student_name}"
    )

    submission_data = {
        "assignment_id": assignment_id,
        "submitted_by": student_id,
        "file_id": document.file_id,
    }
    submissions_collection.insert_one(submission_data)

    await message.reply_text("Assignment submitted successfully!")

# 🛑 /stopedit Command - Cancel Editing Session
@app.on_message(filters.command("stopedit") & filters.private)
async def stop_edit(client, message):
    user_id = message.from_user.id
    if user_id in ongoing_edits:
        del ongoing_edits[user_id]
        await message.reply("✅ Editing session **stopped** successfully.")
    else:
        await message.reply("❌ You are not in an active editing session.")

# 📝 /edit Command - Get Quiz & Show Edit Buttons
@app.on_message(filters.command("edit") & filters.private)
async def edit_quiz(client, message):
    args = message.text.split()

    if len(args) < 2:
        await message.reply("❌ Please provide a valid **Question Set ID**.\nExample: `/edit 12345`")
        return
    user_id = message.from_user.id
    owner_id = OWNER_ID
    
    question_set_id = args[1]
    quiz = questions_collection.find_one({"question_set_id": question_set_id})

    if not quiz:
        quiz = qc_2.find_one({"question_set_id": question_set_id})
    if not quiz:
        await message.reply("❌ No quiz found with this ID.")
        return

    user_id = message.from_user.id
    owner_id = OWNER_ID
    
    if quiz["creator_id"] != user_id and user_id != owner_id:
        await message.reply("❌ **This is not your quiz!** You can only edit quizzes you created.")
        return
    

    ongoing_edits[user_id] = {"question_set_id": question_set_id}

    keyboard_buttons = [
    [
        InlineKeyboardButton("📌 Edit Quiz Name", callback_data=f"edit_title_{question_set_id}"),
        InlineKeyboardButton("⏳ Edit Timer", callback_data=f"edit_timer_{question_set_id}")
    ],
    [
        InlineKeyboardButton("🏴‍☠️ -ve Marking", callback_data=f"edit_negative_{question_set_id}"),
        InlineKeyboardButton("📖 Edit Questions", callback_data=f"edit_questions_{question_set_id}")
    ],
    [
        InlineKeyboardButton("🔀 Shuffle", callback_data=f"shuffle_{question_set_id}")
    ]
        
    ]

    
    
    if not quiz.get("sections", []):  # If sections is empty, show these buttons
        keyboard_buttons.append([
            InlineKeyboardButton("➕ Add Question", callback_data=f"add_question_{question_set_id}"),
            InlineKeyboardButton("➖ Delete Question", callback_data=f"delete_question_{question_set_id}")
        ])

    keyboard = InlineKeyboardMarkup(keyboard_buttons)
    await message.reply(f"📝 **Editing Quiz: {quiz['quiz_name']}**\n\nSelect what you want to edit, I am smart enough to deal with these stuffs 😎:", reply_markup=keyboard)

# 🎯 Handling Button Clicks
@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data

    if user_id not in ongoing_edits:
        return

    question_set_id = ongoing_edits[user_id]["question_set_id"]

    if data.startswith("edit_title_"):
        await callback_query.message.edit("✍️ Send the new **Quiz Name**:")
        ongoing_edits[user_id]["field"] = "quiz_name"

    elif data.startswith("edit_timer_"):
        await callback_query.message.edit("⏳ Send the new **Timer** (in seconds):")
        ongoing_edits[user_id]["field"] = "timer"

    elif data.startswith("edit_negative_"):
        await callback_query.message.edit("➖ Send the new **Negative Marking** (e.g., 0.25):")
        ongoing_edits[user_id]["field"] = "negative_marking"

    elif data.startswith("edit_questions_"):
        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        total_questions = len(quiz["questions"])
        await callback_query.message.edit(f"📖 There are `{total_questions}` questions.\n\nSend the **Question Number** you want to edit:")
        ongoing_edits[user_id]["field"] = "question_number"

    elif data.startswith("add_question_"):
        await callback_query.message.edit("📝 Send the new question in the following format:\n\n"
                                          "Question Text\n"
                                          "Option 1\n"
                                          "Option 2 ✅\n"
                                          "Option 3\n"
                                          "Ex: Explanation (optional)")
        ongoing_edits[user_id]["field"] = "add_question"

    elif data.startswith("delete_question_"):
        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        total_questions = len(quiz["questions"])
        await callback_query.message.edit(f"➖ There are `{total_questions}` questions.\n\n"
                                           "Send the **Question Number** you want to delete:")
        ongoing_edits[user_id]["field"] = "delete_question"


    elif data.startswith("shuffle_"):
        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        shuffle_questions_enabled = quiz.get("shuffle", False)
        shuffle_options_enabled = quiz.get("shuffle_options", False)
        shuffle_questions_text = "Shuffle Questions ✅" if shuffle_questions_enabled else "Shuffle Questions"
        shuffle_options_text = "Shuffle Options ✅" if shuffle_options_enabled else "Shuffle Options"
        keyboard_buttons = [
            [InlineKeyboardButton(shuffle_options_text, callback_data=f"other_shuffle_{question_set_id}")]
        ]

        if not quiz.get("sections", []):  # If sections list is empty, show "Shuffle Questions"
            shuffle_questions_text = "Shuffle Questions ✅" if shuffle_questions_enabled else "Shuffle Questions"
            keyboard_buttons.insert(0, [InlineKeyboardButton(shuffle_questions_text, callback_data=f"edit_shuffle_{question_set_id}")])
        
        keyboard = InlineKeyboardMarkup(keyboard_buttons)
        
        await callback_query.message.edit("🔀 Select a shuffle option:", reply_markup=keyboard)

    elif data.startswith("edit_shuffle_"):

        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        new_shuffle_value = not quiz.get("shuffle", False)
        questions_collection.update_one(
            {"question_set_id": question_set_id},
            {"$set": {"shuffle": new_shuffle_value}}
        )
        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        shuffle_questions_enabled = quiz.get("shuffle", False)
        shuffle_options_enabled = quiz.get("shuffle_options", False)
        shuffle_questions_text = f"Shuffle Questions {'✅' if shuffle_questions_enabled else ''}"
        shuffle_options_text = f"Shuffle Options {'✅' if shuffle_options_enabled else ''}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(shuffle_questions_text, callback_data=f"edit_shuffle_{question_set_id}")],
            [InlineKeyboardButton(shuffle_options_text, callback_data=f"other_shuffle_{question_set_id}")]
        ])
        await callback_query.message.edit_text("🔀 Select a shuffle option:", reply_markup=keyboard)
    
    elif data.startswith("other_shuffle_"):

        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        new_shuffle_value = not quiz.get("shuffle_options", False)
        questions_collection.update_one(
            {"question_set_id": question_set_id},
            {"$set": {"shuffle_options": new_shuffle_value}}
        )
        quiz = questions_collection.find_one({"question_set_id": question_set_id})
        shuffle_questions_enabled = quiz.get("shuffle", False)
        shuffle_options_enabled = quiz.get("shuffle_options", False)
        shuffle_questions_text = f"Shuffle Questions {'✅' if shuffle_questions_enabled else ''}"
        shuffle_options_text = f"Shuffle Options {'✅' if shuffle_options_enabled else ''}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(shuffle_questions_text, callback_data=f"edit_shuffle_{question_set_id}")],
            [InlineKeyboardButton(shuffle_options_text, callback_data=f"other_shuffle_{question_set_id}")]
        ])
        await callback_query.message.edit_text("🔀 Select a shuffle option:", reply_markup=keyboard)

async def extract_quiz_questions(app: Client, url_message: str, user_id: int, log_group_id: int, user_quiz_data: dict):
    """
    Extract quiz questions from URL using PHP API and process them for Pyrogram quiz.
    
    Args:
        app: Pyrogram Client instance
        url_message: Message containing URL and range (e.g., "https://rojgarwithankit.co.in/test-series/589/test-ssc/30109/terms 12-20")
        user_id: User ID for storing quiz data
        log_group_id: Log group chat ID for uploading images
        user_quiz_data: Global dictionary to append questions to
    
    Returns:
        int: Number of questions extracted
    """
    

    parts = url_message.strip().split()
    url = parts[0]
    question_range = parts[1] if len(parts) > 1 else None
    subject_id = None
    
    url_pattern = r'/test-series/(\d+)/[^/]+/(\d+)'
    match = re.search(url_pattern, url)
    subject_match = re.search(r'[?&]subjectId=(\d+)', url)
    subject_id = subject_match.group(1) if subject_match else None
    if not match:
        raise ValueError(
            "Invalid URL format. Expected format: "
            "/test-series/{series_id}/<any>/{test_id}/terms"
            )
    
    test_series_id = match.group(1)
    test_id = match.group(2)
    

    start_idx = 0
    end_idx = None
    
    if question_range:
        range_match = re.match(r'(\d+)-(\d+)', question_range)
        if range_match:
            start_idx = int(range_match.group(1)) - 1  # Convert to 0-based index
            end_idx = int(range_match.group(2))
    

    api_url = "purchase the api"
    
    try:
        params = {
        'test_series_id': test_series_id,
        'test_id': test_id,
        'user_id': user_id
        }

        if subject_id:
            params['subject_id'] = subject_id
        
        api_response = requests.get(
            api_url,
            params=params,
            timeout=30
        )
        api_response.raise_for_status()
        api_data = api_response.json()
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch from PHP API: {str(e)}")
    

    if api_data.get('status') != 'success':
        error_msg = api_data.get('message', 'Unknown error from API')
        raise Exception(f"API Error: {error_msg}")
    

    questions_url = api_data.get('questions_url')
    if not questions_url:
        raise ValueError("Questions URL not found in API response")
    

    try:
        questions_response = requests.get(questions_url, timeout=30)
        questions_response.raise_for_status()
        questions_data = questions_response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch questions: {str(e)}")
    

    if end_idx:
        questions_to_process = questions_data[start_idx:end_idx]
    else:
        questions_to_process = questions_data[start_idx:]
    

    if user_id not in user_quiz_data:
        user_quiz_data[user_id] = {"questions": []}
    

    processed_count = 0
    

    for q_data in questions_to_process:
        try:

            has_option_image = any([
                q_data.get(f'option_image_{i}', '').strip() 
                for i in range(1, 11)
            ])
            
            if has_option_image:
                print(f"Skipping question {q_data.get('id')} - contains option images")
                continue
            

            question_html = q_data.get('question', '')
            question_text = clean_html(question_html)
            

            options = []
            for i in range(1, 11):
                option_html = q_data.get(f'option_{i}', '').strip()
                if option_html:
                    option_text = clean_html(option_html)
                    options.append(option_text)
            

            if len(options) < 2 or len(options) > 10:
                print(f"Skipping question {q_data.get('id')} - invalid number of options: {len(options)}")
                continue
            

            correct_answer = q_data.get('answer', '1')
            correct_option_index = int(correct_answer) - 1
            

            solution_html = q_data.get('solution_text', '')
            explanation = clean_html(solution_html)
            

            file_id = None
            image_links = [
                q_data.get('image_link_1', '').strip(),
                q_data.get('image_link_2', '').strip(),
                q_data.get('image_link_3', '').strip()
            ]
            

            for img_url in image_links:
                if img_url:
                    try:
                        file_id = await upload_image_to_log_group(app, img_url, log_group_id)
                        if file_id:
                            break
                    except Exception as e:
                        print(f"Failed to upload image {img_url}: {e}")
            

            user_quiz_data[user_id]["questions"].append({
                "question": question_text,
                "options": options,
                "correct_option_id": correct_option_index,
                "explanation": explanation,
                "reply_text": "",  # No reply text needed
                "file_id": file_id,
            })
            
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing question {q_data.get('id')}: {e}")
            continue
    
    return processed_count

async def upload_image_to_log_group(app: Client, image_url: str, log_group_id: int) -> str:
    """
    Upload image to log group and return file_id.
    
    Args:
        app: Pyrogram Client instance
        image_url: URL of the image to upload
        log_group_id: Chat ID of the log group
    
    Returns:
        str: file_id of the uploaded image
    """

    response = requests.get(image_url, timeout=10)
    response.raise_for_status()
    

    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
        tmp_file.write(response.content)
        tmp_path = tmp_file.name
    
    try:

        message = await app.send_photo(
            chat_id=log_group_id,
            photo=tmp_path,
            caption=f"Quiz image from: {image_url}"
        )
        

        file_id = message.photo.file_id
        
        return file_id
    
    finally:

        if os.path.exists(tmp_path):
            os.remove(tmp_path)

async def handle_rojgar_link(client: Client, message: Message, log_group_id: int, user_quiz_data: dict):
    """
    Handler for rojgarwithankit.co.in links.
    Extracts quiz questions and appends them to user_quiz_data.
    
    Args:
        client: Pyrogram Client instance
        message: Message object
        log_group_id: Log group chat ID for uploading images
        user_quiz_data: Dictionary to append questions to
    
    Returns:
        int: Number of questions extracted
    """
    user_id = message.from_user.id
    

    url_message = message.text
    

    processing_msg = await message.reply_text("⏳ Processing quiz questions...")
    
    try:
        processed_count = await extract_quiz_questions(
            app=client,
            url_message=url_message,
            user_id=user_id,
            log_group_id=log_group_id,
            user_quiz_data=user_quiz_data
        )
        
        if processed_count == 0:
            await processing_msg.edit_text("❌ No valid questions found in the specified range.")
            return 0
        
        await processing_msg.edit_text(
            f"✅ Extracted {processed_count} questions successfully!\n"
            f"Total questions in queue: {len(user_quiz_data[user_id]['questions'])}"
        )
        
        return processed_count
        
    except Exception as e:
        await processing_msg.edit_text(f"❌ Error: {str(e)}")
        print(f"Error in handle_rojgar_link: {e}")
        return 0

@app.on_message(
    (filters.text | filters.poll) & 
    filters.private & 
    ~filters.command([
        "start", "create", "myquizzes", "pause", "features", "gcast", "fast", "slow", "normal",
        "stopcast", "resume", "edit", "info", "ban", "done", "assignment", "submit",
        "remall", "del", "remove", "clearlist", "stats", "help", "stop", "stopedit", "cancel", "ocr", "login", "quiz", "addfilter", "listfilters", "removefilter", "quizhelp"
    ])
)
async def handle_all_messages(client, message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    

    if user_id in ongoing_edits:
        question_set_id = ongoing_edits[user_id]["question_set_id"]
        field = ongoing_edits[user_id].get("field")
        
        if field in ["quiz_name", "timer", "negative_marking"]:
            new_value = message.text.strip()
            

            if field == "timer":
                if not new_value.isdigit() or int(new_value) <= 9:
                    await message.reply("❌ Invalid timer! Please enter a number greater than 9 seconds.")
                    return
                new_value = int(new_value)

            elif field == "quiz_name":
                new_value = new_value

            elif field == "negative_marking":
                try:
                    if new_value.isdigit():
                        new_value = int(new_value)
                    else:
                        new_value = float(fractions.Fraction(new_value)) if "/" in new_value else float(new_value)
                    
                    if new_value >= 1 or new_value < 0:
                        await message.reply("❌ Negative marking cannot be less than 0 and greater or equal to 1! Send again...")
                        return
                except ValueError:
                    await message.reply("❌ Invalid input! Please enter a number.")
                    return
            

            questions_collection.update_one(
                {"question_set_id": question_set_id},
                {"$set": {field: new_value}}
            )
            
            await message.reply("✅ **Updated Successfully!**")
            del ongoing_edits[user_id]
            
        elif field == "question_number":
            quiz = questions_collection.find_one({"question_set_id": question_set_id})
            total_questions = len(quiz["questions"])
            
            try:
                question_index = int(message.text.strip()) - 1
                if question_index < 0 or question_index >= total_questions:
                    await message.reply("❌ Invalid question number! Please enter a valid number.")
                    return
            except ValueError:
                await message.reply("❌ Please enter a valid number.")
                return
                
            ongoing_edits[user_id]["question_index"] = question_index
            await message.reply(f"📝 Send the **updated question text** for Question `{question_index + 1}`:")
            ongoing_edits[user_id]["field"] = "update_question_text"
            
        elif field == "update_question_text":
            question_index = ongoing_edits[user_id]["question_index"]
            reply_message = message.reply_to_message
            reply_text = reply_message.text if reply_message and reply_message.text else None
            file_id = None
            
            if reply_message and reply_message.photo:
                copied_message = await client.copy_message(
                    chat_id=message.chat.id,
                    from_chat_id=reply_message.chat.id,
                    message_id=reply_message.id,
                )
                file_id = copied_message.photo.file_id
                
            new_question_text = message.text.split("\n")
            question_text = new_question_text[0].strip()
            options = []
            correct_option_id = None
            explanation = None
            
            for i, line in enumerate(new_question_text[1:]):
                if line.startswith("Ex:"):
                    explanation = line[3:].strip()
                    break
                if "✅" in line:
                    correct_option_id = len(options)
                    options.append(line.replace("✅", "").strip())
                else:
                    options.append(line.strip())
                    
            if not question_text or len(options) < 2 or correct_option_id is None:
                await message.reply("❌ Invalid question format. Please follow the correct format.")
                return
                

            questions_collection.update_one(
                {"question_set_id": question_set_id},
                {"$set": {f"questions.{question_index}": {
                    "question": question_text,
                    "options": options,
                    "correct_option_id": correct_option_id,
                    "explanation": explanation,
                    "file_id": file_id,
                    "reply_text": reply_text
                }}}
            )
            
            await message.reply("✅ **Question Updated Successfully!**")
            del ongoing_edits[user_id]
            
        elif field == "add_question":
            new_question_text = message.text.split("\n")
            question_text = new_question_text[0].strip()
            options = []
            correct_option_id = None
            explanation = None
            
            for i, line in enumerate(new_question_text[1:]):
                if line.startswith("Ex:"):
                    explanation = line[3:].strip()
                    break
                if "✅" in line:
                    correct_option_id = len(options)
                    options.append(line.replace("✅", "").strip())
                else:
                    options.append(line.strip())
                    
            if not question_text or len(options) < 2 or correct_option_id is None:
                await message.reply("❌ Invalid question format. Please follow the correct format.")
                return
                

            questions_collection.update_one(
                {"question_set_id": question_set_id},
                {"$push": {"questions": {
                    "question": question_text,
                    "options": options,
                    "correct_option_id": correct_option_id,
                    "explanation": explanation,
                    "file_id": None,
                    "reply_text": None
                }}}
            )
            
            await message.reply("✅ **Question Added Successfully!**")
            del ongoing_edits[user_id]
            
        elif field == "delete_question":
            try:
                input_text = message.text.strip()
                

                if input_text.isdigit():
                    question_index = int(input_text) - 1
                    quiz = questions_collection.find_one({"question_set_id": question_set_id})
                    total_questions = len(quiz["questions"])
                    
                    if question_index < 0 or question_index >= total_questions:
                        await message.reply("❌ Invalid question number! Please enter a valid number.")
                        return
                    

                    questions_collection.update_one(
                        {"question_set_id": question_set_id},
                        {"$pull": {"questions": quiz["questions"][question_index]}}
                    )
                    await message.reply("✅ **Question Deleted Successfully!**")
                

                elif "-" in input_text:
                    parts = input_text.split("-")
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        start = int(parts[0]) - 1
                        end = int(parts[1]) - 1
                        
                        quiz = questions_collection.find_one({"question_set_id": question_set_id})
                        total_questions = len(quiz["questions"])
                        
                        if start < 0 or end >= total_questions or start > end:
                            await message.reply("❌ Invalid range! Please enter a valid range (e.g., 1-10).")
                            return
                        

                        questions_to_keep = [
                            q for idx, q in enumerate(quiz["questions"])
                            if idx < start or idx > end
                        ]
                        

                        questions_collection.update_one(
                            {"question_set_id": question_set_id},
                            {"$set": {"questions": questions_to_keep}}
                        )
                        
                        deleted_count = end - start + 1
                        await message.reply(f"✅ **Deleted {deleted_count} questions successfully!**")
                    else:
                        await message.reply("❌ Invalid range format! Please use format like '1-10'.")
                else:
                    await message.reply("❌ Invalid input! Please enter a number (e.g., 5) or range (e.g., 1-10).")
                    
                del ongoing_edits[user_id]
                
            except ValueError:
                await message.reply("❌ Please enter a valid number or range.")
            except Exception as e:
                print(f"Error deleting questions: {str(e)}")
                await message.reply("❌ An error occurred while deleting questions.")
        
        return
    

    if user_id in user_quiz_data:
        user_data = user_quiz_data[user_id]
        

        if message.poll:
            poll = message.poll
            

            user_db = users_collection.find_one({"chat_id": chat_id})
            remove_words = user_db.get("remove_words", []) if user_db else []
            
            if poll.type != PollType.QUIZ:
                await message.reply("Invalid poll detected, send Quiz Type Poll")
                return
                
            question = filter_words(poll.question, remove_words)
            question = await remove_baby(question)
            
            options = [filter_words(option.text, remove_words) for option in poll.options]
            
            correct_option_id = 1
            if hasattr(poll, 'correct_option_id'):
                correct_option_id = poll.correct_option_id
            
            description = None
            if hasattr(poll, 'explanation'):
                description = filter_words(poll.explanation, remove_words)
                description = await remove_baby(description)
                
            reply_message = message.reply_to_message
            reply_text = reply_message.text if reply_message and reply_message.text else None
            file_id = None
            
            if reply_message and reply_message.photo:
                copied_message = await client.copy_message(
                    chat_id=BOT_GROUP,
                    from_chat_id=reply_message.chat.id,
                    message_id=reply_message.id,
                )
                file_id = copied_message.photo.file_id
                

            user_quiz_data[user_id]["questions"].append({
                "question": question,
                "options": options,
                "correct_option_id": correct_option_id,
                "explanation": description,
                "file_id": file_id,
                "reply_text": reply_text
            })
            
            total = len(user_quiz_data[user_id]["questions"])
            await message.reply(f"✅ {total} Question saved! Send the next poll / TestBook Test Link or question set or type /done when finished or /cancel to cancel the quiz creation.")
            await asyncio.sleep(2)
            return
            

        if user_data.get("awaiting_name"):
            quiz_name = message.text.strip()
            if not quiz_name:
                await message.reply("❌ Invalid quiz name. Please send a valid name.")
                return
                
            user_quiz_data[user_id]["quiz_name"] = quiz_name
            user_quiz_data[user_id]["awaiting_name"] = False
            await message.reply(f"How many questions do you want?")
            user_quiz_data[user_id]["awaiting_no_of_questions"] = True
            return

        if user_data.get("awaiting_no_of_questions"):
            no_of_questions = message.text.strip()
            if not no_of_questions.isdigit():
                await message.reply(f"Set the number of expected questions to a valid number")
                return

            user_quiz_data[user_id]["no_of_questions"] = int(no_of_questions)
            user_quiz_data[user_id]["awaiting_no_of_questions"] = False
            await message.reply(f"""✅ Quiz name set to: **{user_quiz_data[user_id]["quiz_name"]}** and **{no_of_questions}** expected\nNow send questions in the stated format, or try to send a quiz poll or .txt file, send /cancel to stop creating quiz.""")
            return
            
        if user_data.get("awaiting_section_choice"):
            choice = message.text.strip().lower()
            if choice not in ["yes", "no"]:
                await message.reply("❌ Invalid response. Please reply with 'yes' or 'no'.")
                return
                
            user_data["section_wise"] = choice == "yes"
            del user_data["awaiting_section_choice"]
            
            if user_data["section_wise"]:
                user_data["awaiting_section_count"] = True
                await message.reply("📌 How many sections do you want? (Must be greater than 1)")
            else:
                user_data["awaiting_timer"] = True
                await message.reply("⏳ Enter the quiz timer in seconds (greater than 10 sec).")
            return
            
        if user_data.get("awaiting_timer"):
            try:
                timer = int(message.text.strip())
                if timer <= 9:
                    raise ValueError
            except ValueError:
                await message.reply("❌ Invalid timer. Please send the time in seconds greater than 9 seconds (e.g., 60).")
                return
                
            user_quiz_data[user_id]["timer"] = timer
            del user_quiz_data[user_id]["awaiting_timer"]
            user_quiz_data[user_id]["awaiting_negative_marking"] = True
            await message.reply("📝 Please send the negative marking if you want to add else send 0.\n\n__eg. Enter an integer, fraction (e.g., 1/3), or decimal (e.g., 0.25).__")
            return
            
        if user_data.get("awaiting_section_count"):
            try:
                section_count = int(message.text.strip())
                if section_count < 2:
                    raise ValueError
            except ValueError:
                await message.reply("❌ Invalid number. Please enter an integer greater than 1.")
                return
                
            user_data["section_count"] = section_count
            user_data["sections"] = []
            user_data["current_section"] = 1
            user_data["last_range_end"] = 0
            del user_data["awaiting_section_count"]
            user_data["awaiting_section_name"] = True
            
            await message.reply(f"📌 Enter the name for section 1:")
            return
            
        if user_data.get("awaiting_section_name"):
            section_name = message.text.strip()
            if not section_name:
                await message.reply("❌ Section name cannot be empty. Please enter a valid name.")
                return
                
            user_data["sections"].append({"name": section_name})
            del user_data["awaiting_section_name"]
            user_data["awaiting_question_range"] = True
            
            total_questions = len(user_data["questions"])
            await message.reply(f"📌 Enter the question range for '{section_name}' (e.g., 1-5). Maximum: {total_questions}")
            return
            
        if user_data.get("awaiting_question_range"):
            try:
                range_text = message.text.strip()
                start, end = map(int, range_text.split("-"))
                total_questions = len(user_data["questions"])
                
                if start < 1 or end > total_questions or start > end:
                    raise ValueError
                    
                if user_data["last_range_end"] and start != user_data["last_range_end"] + 1:
                    raise ValueError("❌ Invalid range. The next section must start immediately after the previous section.")
            except ValueError as e:
                error_msg = str(e) if isinstance(e, ValueError) and str(e) != "invalid literal for int() with base 10" else "❌ Invalid range. Ensure numbers are within total questions and properly formatted (e.g., 1-5)."
                await message.reply(error_msg)
                return
                
            user_data["sections"][-1]["question_range"] = (start, end)
            del user_data["awaiting_question_range"]
            user_data["last_range_end"] = end
            user_data["awaiting_section_timer"] = True
            
            await message.reply(f"⏳ Enter the timer for this section (greater than 10 sec).")
            return
            
        if user_data.get("awaiting_section_timer"):
            try:
                timer = int(message.text.strip())
                if timer <= 10:
                    raise ValueError
            except ValueError:
                await message.reply("❌ Invalid timer. Enter a value greater than 10 seconds.")
                return
                
            user_data["sections"][-1]["timer"] = timer
            del user_data["awaiting_section_timer"]
            
            if len(user_data["sections"]) < user_data["section_count"]:
                user_data["current_section"] += 1
                await message.reply(f"📌 Enter the name for section {user_data['current_section']}:")
                user_data["awaiting_section_name"] = True
            else:
                user_data["awaiting_negative_marking"] = True
                await message.reply("📝 Please send the negative marking if you want to add else send 0. \n__eg. Enter an integer, fraction (e.g., 1/3), or decimal (e.g., 0.25).__")
            return
            
        if user_data.get("awaiting_negative_marking"):
            try:
                input_text = message.text.strip()
                if input_text.isdigit():
                    negative_marking = int(input_text)
                else:
                    negative_marking = float(fractions.Fraction(input_text)) if "/" in input_text else float(input_text)
                    
                if negative_marking < 0 or negative_marking >= 1:
                    raise ValueError
            except ValueError:
                await message.reply("❌ Invalid negative marking value. Please enter a value between 0 and less than 1 (e.g., 1/3, 0.25).")
                return
                
            user_quiz_data[user_id]["negative_marking"] = negative_marking
            del user_quiz_data[user_id]["awaiting_negative_marking"]

            user = message.from_user.mention
            quiz_name = user_quiz_data[user_id]["quiz_name"]
            timer = user_quiz_data[user_id].get("timer", None)
            sections = user_quiz_data[user_id].get("sections", [])
            negative_marking = user_quiz_data[user_id]["negative_marking"]
            question_set_id = generate_random_id()
            

            questions_collection.insert_one({
                "question_set_id": question_set_id,
                "creator_id": user_id,
                "quiz_name": quiz_name,
                "questions": user_quiz_data[user_id]["questions"],
                "sections": sections,
                "timer": timer,
                "negative_marking": negative_marking,
            })
            

            del user_quiz_data[user_id]
            gc.collect()
            

            quiz_text = (
                f"> **Quiz Created Successfully!**\n\n"
                f"**💳 Quiz Name:** {quiz_name}\n"
                f"**#️⃣ Questions:** {len(questions_collection.find_one({'question_set_id': question_set_id})['questions'])}\n"
                f"**⏰ Timer:** {timer} seconds\n"
                f"**🆔 Quiz ID:** `{question_set_id}`\n"
                f"**🏴‍☠️ -ve Marking:** `{negative_marking:.2f}`\n"
                f"**🧒 Creator:** `{user}`"
            )
            

            if sections:
                quiz_text += "\n\n> **📂 Sections :**"
                for i, section in enumerate(sections, start=1):
                    section_name = section["name"]
                    start_idx, end_idx = section["question_range"]
                    section_timer = section.get("timer", "Not specified")
                    quiz_text += (
                        f"\n\n**Section {i}:** {section_name}\n"
                        f"  - **Questions:** {start_idx} to {end_idx}\n"
                        f"  - **Timer:** {section_timer} seconds"
                    )
            

            start_deep_link = f"https://t.me/NewSamghibili_bot?start={question_set_id}"
            group_start_deep_link = f"https://t.me/NewSamghibili_bot?startgroup={question_set_id}"
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Start Quiz Now", url=start_deep_link)],
                [InlineKeyboardButton("🚀 Start Quiz in Group", url=group_start_deep_link)],
                [InlineKeyboardButton("🔗 Share Quiz", switch_inline_query=question_set_id)]
            ])
            

            await message.reply(quiz_text, reply_markup=keyboard)
            fresh_text = await remove_baby(quiz_text)
            await app.send_message(BOT_GROUP, fresh_text, reply_markup=keyboard)
            return
            
        if "rojgarwithankit.co.in" in message.text and "/test-series/" in message.text:
            await handle_rojgar_link(client, message, BOT_GROUP, user_quiz_data)
            return
            

        questions_blocks = message.text.split("\n\n")
        reply_message = message.reply_to_message
        reply_text = reply_message.text if reply_message and reply_message.text else None
        
        for block in questions_blocks:
            lines = [line.strip() for line in block.strip().split("\n") if line.strip()]
            if not lines:
                continue
                
            question = ""
            options = []
            correct_option_id = None
            explanation = None
            file_id = None
            
            if reply_message and reply_message.photo:
                copied_message = await client.copy_message(
                    chat_id=BOT_GROUP,
                    from_chat_id=reply_message.chat.id,
                    message_id=reply_message.id,
                )
                file_id = copied_message.photo.file_id
            

            options_marker_idx = None
            explanation_idx = None
            
            for idx, line in enumerate(lines):
                if line.lower() in ["options", "options:", "option", "option:", "👉 Choose Correct Option"]:
                    options_marker_idx = idx
                elif line.startswith("Ex:") or line.startswith("Explanation:"):
                    explanation_idx = idx
                    break
            

            if options_marker_idx is not None:

                question = "\n".join(lines[:options_marker_idx]).strip()
                

                start_idx = options_marker_idx + 1
                end_idx = explanation_idx if explanation_idx else len(lines)
                
                for line in lines[start_idx:end_idx]:

                    cleaned = line
                    if len(line) > 2 and line[0].isalnum() and line[1] in ['.', ')', ':']:
                        cleaned = line[2:].strip()
                    elif len(line) > 3 and line[0].isalnum() and line[1] == line[2] and line[1] in ['.', ')']:
                        cleaned = line[3:].strip()
                    
                    if "✅" in cleaned:
                        correct_option_id = len(options)
                        options.append(cleaned.replace("✅", "").strip())
                    else:
                        options.append(cleaned)
                

                if explanation_idx:
                    explanation = lines[explanation_idx]
                    if explanation.startswith("Ex:"):
                        explanation = explanation[3:].strip()
                    elif explanation.startswith("Explanation:"):
                        explanation = explanation[12:].strip()

            else:
                question = lines[0].strip()
                        
                
                for i, line in enumerate(lines[1:]):
                    if line.startswith("Ex:") or line.startswith("Explanation:"):
                        if line.startswith("Ex:"):
                            explanation = line[3:].strip()
                        else:
                            explanation = line[12:].strip()
                        break
                    

                    cleaned = line
                    if len(line) > 2 and line[0].isalnum() and line[1] in ['.', ')', ':']:
                        cleaned = line[2:].strip()
                    elif len(line) > 3 and line[0].isalnum() and line[1] == line[2] and line[1] in ['.', ')']:
                        cleaned = line[3:].strip()
                    
                    if "✅" in cleaned:
                        correct_option_id = len(options)
                        options.append(cleaned.replace("✅", "").strip())
                    else:
                        options.append(cleaned)
            if not question or len(options) < 2 or correct_option_id is None:
                await message.reply("❌ Invalid question format in one of the questions. Please follow the correct format.")
                return
                
            user_quiz_data[user_id]["questions"].append({
                "question": question,
                "options": options,
                "correct_option_id": correct_option_id,
                "explanation": explanation,
                "file_id": file_id,
                "reply_text": reply_text
            })
            
        total = len(user_quiz_data[user_id]["questions"])
        if total > 100:
            await message.reply(f"✅ Reached {total} soon getting 200 it is advised to stop here...")
            return
        await message.reply(f"✅ {total} Questions saved! Send the next question set, .txt or quiz poll or type /done when finished or /cancel to cancel")

@app.on_message(filters.private & filters.reply)
async def handle_creator_reply(client, message):
    if not message.reply_to_message or not message.reply_to_message.caption:

        return

    caption_lines = message.reply_to_message.caption.split("\n")
    
    student_id_line = next((line for line in caption_lines if "🆔 Student ID:" in line), None)
    student_name_line = next((line for line in caption_lines if "👨‍🎓 Student Name:" in line), None)
    assignment_id_line = next((line for line in caption_lines if "🔖 Assignment ID:" in line), None)

    if not student_id_line:

        return

    student_id = int(student_id_line.split(":")[1].strip())  # Extract student ID
    student_name = student_name_line.split(":")[1].strip() if student_name_line else "Student"

    await client.send_message(
        student_id,
        f"Hello {student_name}, you got creator's reply fro assignment ID : `{assignment_id_line}`\n\n{message.text}"
    )

    await message.reply_text("Your reply has been sent to the student.")

app.run()
