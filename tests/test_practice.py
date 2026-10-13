"""Small exhaustive oracles catch boundary errors beyond example memorization."""

import itertools
from collections import Counter

import pytest

from practice.solutions import (
    compress,
    group_anagrams,
    is_palindrome,
    longest_unique_substring,
    minimum_window,
)


def all_strings(alphabet, max_length):
    for length in range(max_length + 1):
        for characters in itertools.product(alphabet, repeat=length):
            yield "".join(characters)


def brute_unique(text):
    return max(
        [0]
        + [
            end - start
            for start in range(len(text))
            for end in range(start + 1, len(text) + 1)
            if len(set(text[start:end])) == end - start
        ]
    )


def brute_window(text, target):
    if not target:
        return ""
    required = Counter(target)
    candidates = []
    for start in range(len(text)):
        for end in range(start + 1, len(text) + 1):
            counts = Counter(text[start:end])
            if all(counts[character] >= count for character, count in required.items()):
                candidates.append((end - start, start, text[start:end]))
    return min(candidates)[2] if candidates else ""


def test_longest_unique_exhaustive_including_unicode():
    for text in all_strings("ab🙂", 6):
        assert longest_unique_substring(text) == brute_unique(text), text


@pytest.mark.parametrize(
    ("text", "expected"),
    [("abcabcbb", 3), ("bbbbb", 1), ("pwwkew", 3), ("abba", 2), ("", 0)],
)
def test_longest_unique_examples(text, expected):
    assert longest_unique_substring(text) == expected


def test_palindrome_exhaustive_ascii_filter_contract():
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for text in all_strings("aA1!é", 5):
        filtered = "".join(character.lower() for character in text if character in allowed)
        assert is_palindrome(text) == (filtered == filtered[::-1]), text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("A man, a plan, a canal: Panama", True),
        ("race a car", False),
        ("", True),
        ("!!!", True),
        ("0P", False),
        ("éa", True),
    ],
)
def test_palindrome_examples(text, expected):
    assert is_palindrome(text) is expected


def test_anagrams_preserve_first_seen_groups_words_and_duplicates():
    words = ["eat", "tea", "tan", "ate", "nat", "bat", "eat", "", ""]
    assert group_anagrams(words) == [
        ["eat", "tea", "ate", "eat"],
        ["tan", "nat"],
        ["bat"],
        ["", ""],
    ]


def test_anagrams_exhaustive_matches_independent_sorted_key_oracle():
    vocabulary = list(all_strings("ab", 2))
    for words in itertools.product(vocabulary, repeat=3):
        expected = {}
        for word in words:
            expected.setdefault("".join(sorted(word)), []).append(word)
        assert group_anagrams(list(words)) == list(expected.values())
    assert group_anagrams([]) == []


@pytest.mark.parametrize("word", ["Eat", "a1", "á", "a b"])
def test_anagrams_reject_characters_outside_lowercase_latin(word):
    with pytest.raises(ValueError):
        group_anagrams([word])


def test_compress_exhaustive_matches_independent_groupby_encoding():
    for text in all_strings("ab🙂", 6):
        expected = []
        for character, group in itertools.groupby(text):
            count = sum(1 for _ in group)
            expected.append(character)
            if count > 1:
                expected.extend(str(count))
        chars = list(text)
        original_id = id(chars)
        length = compress(chars)
        assert id(chars) == original_id
        assert length == len(expected)
        assert chars[:length] == expected, text
        assert len(chars) == len(text)


def test_compress_handles_multiple_count_digits_and_digit_characters():
    chars = list("a" * 12 + "1" * 10 + "b")
    length = compress(chars)
    assert chars[:length] == list("a12110b")
    assert length == 7


def test_minimum_window_exhaustive_counts_unicode_and_earliest_ties():
    targets = list(all_strings("ab🙂", 3))
    for text in all_strings("ab🙂", 4):
        for target in targets:
            assert minimum_window(text, target) == brute_window(text, target), (text, target)


@pytest.mark.parametrize(
    ("text", "target", "expected"),
    [
        ("ADOBECODEBANC", "ABC", "BANC"),
        ("a", "aa", ""),
        ("", "a", ""),
        ("anything", "", ""),
        ("abxba", "ab", "ab"),
        ("xy🙂é🙂z", "🙂🙂", "🙂é🙂"),
        ("baaab", "aab", "baa"),
    ],
)
def test_minimum_window_examples(text, target, expected):
    assert minimum_window(text, target) == expected
