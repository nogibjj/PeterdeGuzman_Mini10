# Test function created in main.py script

from main import double


def test_double():
    # testing out double function
    assert double(4) % 2 == 0
    assert double(11) % 2 == 0
    assert double(100) % 2 == 0


if __name__ == "__main__":
    test_double()
