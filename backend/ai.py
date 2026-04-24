import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).with_name(".env"))


def fallback_explanation(customer):
    return (
        f"{customer['customer']} needs attention because it has been "
        f"{customer['days_since_last']} days since their last order, compared "
        f"with their usual average gap of {customer['avg_gap']} days. "
        f"Recommended action: {customer['action']}."
    )


def generate_customer_explanation(customer):
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return fallback_explanation(customer)

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)

        response = client.responses.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a sales assistant. Explain customer order "
                        "risk in one short, practical sentence. Do not invent "
                        "facts. Base your answer only on the data provided."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Customer: {customer['customer']}\n"
                        f"Average order gap: {customer['avg_gap']} days\n"
                        f"Days since last order: {customer['days_since_last']}\n"
                        f"Priority score: {customer['priority_score']}\n"
                        f"Recommended action: {customer['action']}"
                    )
                }
            ],
            max_output_tokens=80
        )

        explanation = response.output_text.strip()

        if not explanation:
            return fallback_explanation(customer)

        return explanation
    except Exception as error:
        print(f"AI explanation failed: {error}")
        return fallback_explanation(customer)


def add_ai_explanations(customers):
    explained_customers = []

    for customer in customers:
        explained_customer = customer.copy()
        explained_customer["explanation"] = generate_customer_explanation(customer)
        explained_customers.append(explained_customer)

    return explained_customers
