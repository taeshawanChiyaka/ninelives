import ninelives as nl


def scrape(url: str) -> dict[str, str]:
    # Replace with an HTTP client call and respect the target site's terms.
    return {"url": url, "status": "placeholder-example"}


if __name__ == "__main__":
    urls = [f"https://example.com/page/{i}" for i in range(100)]
    print(len(nl.resumable_map(scrape, urls, name="scrape-demo", retries=3)))
