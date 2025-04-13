import sys
import csv

def generate_csv(numbers):
    with open("dataset.csv", "w") as file:
        writer = csv.writer(file)
        writer.writerow(["id"])
        for number in range(1, numbers + 1):
            writer.writerow([number])

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <count>")
        sys.exit(1)
    
    try:
        count = int(sys.argv[1])
        if count <= 0:
            raise ValueError("Count must be a positive integer.")
        
        generate_csv(count)
    except ValueError as e:
        print(e)
        sys.exit(1)
