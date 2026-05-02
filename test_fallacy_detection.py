import json
from fallacy_detector import FallacyAnalyzer
from analyzer_ollama import OllamaAnalyzer
from config import config

def test_fallacy_detection():
    print("🧪 Starting Fallacy Detection Test Suite...")

    analyzer = OllamaAnalyzer(config)
    fallacy_detector = FallacyAnalyzer(config, analyzer)

    # Load some test cases from inventory
    with open('fallacies_inventory.json', 'r', encoding='utf-8') as f:
        inventory = json.load(f)

    # Pick 5 diverse test cases
    test_ids = ["F001", "F041", "F067", "F076", "F081"]
    test_cases = [f for f in inventory if f['id'] in test_ids and f['example']]

    successes = 0
    total = len(test_cases)

    for case in test_cases:
        print(f"\n📝 Testing {case['id']}: {case['name']}")
        print(f"💬 Example: {case['example']}")

        # Analyze
        result = fallacy_detector.analyze_fallacies(case['example'])

        detected_ids = [f['id'] for f in result.get('fallacies', [])]
        print(f"🔍 Detected: {detected_ids}")

        if case['id'] in detected_ids:
            print("✅ SUCCESS: Target fallacy detected.")
            successes += 1
        else:
            print("❌ FAILURE: Target fallacy not detected.")
            print(f"Reasoning: {result.get('reasoning', 'N/A')}")

    print(f"\n📊 Results: {successes}/{total} ({successes/total*100:.1f}%)")

if __name__ == "__main__":
    try:
        test_fallacy_detection()
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        print("Note: This test requires active Ollama and Qdrant services.")
