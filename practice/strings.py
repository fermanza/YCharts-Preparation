"""Five interview exercises. Implement these before reading solutions.py.

Use ``uv run pytest tests/test_practice.py`` to verify the reference solutions.
To test your answers, change its import from practice.solutions to practice.strings.
"""


def longest_unique_substring(text: str) -> int:
    """Return the longest substring length with no repeated Unicode character.

    Examples: "abcabcbb" -> 3; "" -> 0. Aim for O(n) time and explain how
    your window moves when a repeated character occurs outside the window.
    """
    raise NotImplementedError("Implement the sliding-window exercise")


def is_palindrome(text: str) -> bool:
    """Ignore ASCII punctuation/spaces and ASCII letter case.

    Keep only characters a-z, A-Z and 0-9. An empty filtered string is a
    palindrome. Example: "A man, a plan, a canal: Panama" -> True.
    """
    raise NotImplementedError("Implement the two-pointer exercise")


def group_anagrams(words: list[str]) -> list[list[str]]:
    """Group lowercase Latin words, preserving first-seen groups and word order.

    Example: ["eat", "tea", "tan", "ate"] -> [["eat", "tea", "ate"], ["tan"]].
    Duplicate and empty words are valid; other characters raise ValueError.
    """
    raise NotImplementedError("Implement the character-frequency exercise")


def compress(chars: list[str]) -> int:
    """Write run-length encoding into chars and return the valid prefix length.

    A single character keeps no count; larger counts use separate decimal
    digits. Example: list("aaabbc") -> prefix list("a3b2c"), length 5.
    The contents past that prefix are irrelevant. Use O(1) extra space.
    """
    raise NotImplementedError("Implement the in-place two-pointer exercise")


def minimum_window(text: str, target: str) -> str:
    """Return the shortest substring containing target's Unicode multiplicities.

    Resolve equal-length ties with the earliest occurrence. Return "" when
    target is empty or no window exists. Example: ("ADOBECODEBANC", "ABC")
    -> "BANC". Aim for O(len(text) + len(target)) time.
    """
    raise NotImplementedError("Implement the counted sliding-window exercise")
