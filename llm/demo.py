from llm.assistant import answer


def main() -> None:
    print("Stock Pipeline Assistant")
    print("Type 'exit' or 'quit' to quit.")

    while True:
        try:
            question = input("\nAsk a question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if question.lower() in {"exit", "quit"}:
            break

        print()
        print(answer(question))


if __name__ == "__main__":
    main()