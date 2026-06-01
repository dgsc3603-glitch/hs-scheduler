import time
import sys

def main():
    print("🚀 High Output Test Started")
    total_lines = 1000000  # 1 Million lines
    
    for i in range(total_lines):
        # Print a line that is roughly 100 characters long
        print(f"[{i:07d}] This is a test log line to verify memory-efficient chunked logging in the scheduler. Line {i}")
        
        # Periodically show progress
        if i % 10000 == 0:
            print(f"📊 Progress: ({i}/{total_lines})")
            
    print("✅ High Output Test Completed")

if __name__ == "__main__":
    main()
