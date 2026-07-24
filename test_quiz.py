import os
import sys
import json
from quiz_generator import QuizConfig, generate_quiz

def mock_telegram_callback(message: str):
    """Simulates what your Telegram bot would receive and print."""
    print(f"[TELEGRAM BOT CALLBACK] {message}")

def run_test(name: str, config: QuizConfig, file_path: str = None):
    print("\n" + "=" * 60)
    print(f"RUNNING TEST: {name}")
    print("=" * 60)
    
    result = generate_quiz(
        cfg=config,
        file_path=file_path,
        status_callback=mock_telegram_callback,
        retries=2,  # Set to 2 for quick testing
        daily_token_limit=1_000_000
    )

    print("\n--- TEST RESULT SUMMARY ---")
    print(f"Status              : {result['status']}")
    print(f"Requested Questions : {result['requested_questions']}")
    print(f"Returned Questions  : {result['returned_questions']}")
    print(f"Time Elapsed        : {result.get('generation_time_seconds', 'N/A')}s")
    print(f"Tokens Used (Call)  : {result.get('tokens_used_this_request', 'N/A')}")
    
    if result.get("usage_today"):
        today = result["usage_today"]
        print(f"Tokens Used (Today) : {today['tokens_used_today']} / {today['daily_limit']} ({today['percent_used']}%)")
    
    if result["error"]:
        print(f"Error Caught        : {result['error']}")

    if result["questions"]:
        print("\n--- SAMPLE FORMATTED QUESTION [0] ---")
        print(json.dumps(result["questions"][0], indent=2))
    
    print("-" * 60)
    return result

if __name__ == "__main__":
    # Ensure API Key is set
    if not os.environ.get("GEMINI_API_KEY"):
        print("❌ ERROR: GEMINI_API_KEY environment variable is not set.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Test 1: Bare Topic (No document attached)
    # ------------------------------------------------------------------
    cfg_topic = QuizConfig(
        topic_or_source="Photosynthesis",
        num_questions=2,
        num_options=4,
        difficulty="Easy",
        languages="English"
    )
    run_test("Bare Topic Generation", cfg_topic)

    # ------------------------------------------------------------------
    # Test 2: File Upload (PDF/Image) - Optional: set path to a real test file
    # ------------------------------------------------------------------
    test_file_path = "sample.pdf"  # Replace with a real PDF or image if you have one
    if os.path.exists(test_file_path):
        cfg_file = QuizConfig(
            topic_or_source="Attached Document",
            num_questions=3,
            difficulty="Medium"
        )
        run_test("File Upload Test", cfg_file, file_path=test_file_path)
    else:
        print(f"\nℹ️ Skipping File Upload Test: '{test_file_path}' not found.")

    # ------------------------------------------------------------------
    # Test 3: Catching Upload Error Gracefully (Invalid File Path)
    # ------------------------------------------------------------------
    cfg_invalid = QuizConfig(topic_or_source="Non-existent File Test")
    run_test("Invalid File Error Handling", cfg_invalid, file_path="non_existent_file.pdf")
