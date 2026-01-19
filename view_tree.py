import os
import sys

def print_tree(path, max_depth, current_depth=0):
    if current_depth >= max_depth:
        return

    try:
        items = os.listdir(path)
    except PermissionError:
        print("│   " * current_depth + "├── [Permission Denied]")
        return

    for item in sorted(items):
        item_path = os.path.join(path, item)
        print("│   " * current_depth + "├── " + item)

        if os.path.isdir(item_path):
            print_tree(item_path, max_depth, current_depth + 1)


def main():
    args = sys.argv[1:]

    # ===== Trường hợp 1: không truyền tham số =====
    if len(args) == 0:
        path = input("Nhập folder: ").strip()
        depth = int(input("Nhập độ sâu: "))

    # ===== Trường hợp 2: truyền 1 tham số =====
    elif len(args) == 1:
        path = args[0]
        depth = 1

    # ===== Trường hợp 3: truyền 2 tham số =====
    elif len(args) == 2:
        path = args[0]
        depth = int(args[1])

    else:
        print("Usage:")
        print("  python view_tree.py")
        print("  python view_tree.py <folder>")
        print("  python view_tree.py <folder> <depth>")
        sys.exit(1)

    # Chuẩn hoá path
    path = os.path.abspath(path)

    if not os.path.isdir(path):
        print(f"Error: '{path}' không phải là thư mục hợp lệ")
        sys.exit(1)

    print_tree(path, depth)


if __name__ == "__main__":
    main()
