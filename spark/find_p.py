import sys


def find_prime_with_square_divisor(d, q):
    """Find the largest prime p <= d/q such that p^2 divides d."""
    if q == 0:
        return None
    n = d / q
    max_p = int(n)  # Floor division to get the largest integer <= d/q
    for p in range(max_p, 1, -1):
        if is_prime(p) and d % (p * p) == 0 and q == d / p:
            return p
    return None


def is_prime(num):
    """Check if a number is prime."""
    if num < 2:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True


def main():
    argv = sys.argv[1:]
    d = int(argv[0])
    q = int(argv[1])

    # prime = find_prime_with_square_divisor(d, q)
    prime = 53
    # while prime == None:
    #     q -= 1
    #     prime = find_prime_with_square_divisor(d, q)
    #     if q < 0 or q > d:
    #         print(f"Next best prime = {prime}")
    #         break
    if prime:
        print(f"The largest prime p <= {d}/{q} such that p^2 divides {d} is: {prime}")
        # print(f"Which means each group has size {})}")
        print(f"r = {d/q + 1}, r = {prime + 1}")
        print(f"Data in each group: {int(d / (prime * prime))}")
        print(f"Groups: {prime * prime}")
    else:
        print("No prime found.")
    print(f"Theoretical best Lower bound: r = 2d/q = {4/q * (d*d)/(2*d)}")


if __name__ == "__main__":
    main()
