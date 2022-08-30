import pytest
from main import adder

@pytest.mark.parametrize("num1, num2", [(5,6), (15,2), (1,8), (30,90)])

def test_testing(num1,num2):
    assert not adder(num1,num2) == num1 - num2

if __name__ == "__main__":
    pytest.main()


    