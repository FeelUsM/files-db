from typing import Self

class MyClass:
    def __init__(self: Self, x: int):
        self.x = x

    def __eq__(self: Self, other: object) -> bool:
        if not isinstance(other, MyClass):
            raise TypeError(f'{type(self)} == {type(other)}')
        return self.x == other.x

print(MyClass(5) == 5)  # ❌ I want mypy to give an error here

def abstract_comparator(a: object, b: object) -> bool:
    return a == b

print(abstract_comparator(MyClass(5), 5))  # ❌ I also want an error here
