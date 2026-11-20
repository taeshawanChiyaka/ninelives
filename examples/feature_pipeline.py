import ninelives as nl


@nl.cache(version="feature-v1", env=["FEATURE_WINDOW"])
def build_features(text: str, window: int = 7) -> tuple[int, int]:
    return len(text), min(len(text), window)


if __name__ == "__main__":
    print(build_features("resumable computation"))
    print(build_features.explain("resumable computation"))
