import ninelives as nl


def embed(text: str) -> list[float]:
    # Replace with your embedding model call.
    return [float(len(text)), float(sum(map(ord, text)) % 997)]


if __name__ == "__main__":
    documents = [f"document {i}" for i in range(50_000)]
    vectors = nl.resumable_map(embed, documents, name="embedding-demo", chunk_size=256)
    print(f"embedded {len(vectors)} documents")
