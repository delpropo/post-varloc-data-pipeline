import os

def create_symlink(source, link_name):
    try:
        os.symlink(source, link_name)
        print(f"Symbolic link created: {link_name} -> {source}")
    except FileNotFoundError:
        print(f"The source path '{source}' does not exist.")
    except OSError as e:
        print(f"Failed to create symbolic link: {e}")

if __name__ == "__main__":
    source_path = input("Enter the source path: ")
    link_name = input("Enter the symbolic link name: ")
    create_symlink(source_path, link_name)