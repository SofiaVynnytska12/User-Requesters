#testApp
import os


test_variable = os.getenv("TEST_VARIABLE", "No value provided")

print(f"Application started! TEST_VARIABLE: {test_variable}")