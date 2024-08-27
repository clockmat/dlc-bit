import hashlib
import os
import shutil


def delete_file(*files):
    for file in files:
        if os.path.exists(file):
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)


def md5hash(name: str) -> str:
    h = hashlib.md5()
    h.update(name.encode("utf-8"))
    return h.hexdigest()


def clean_empty_dirs(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for name in dirs:
            dir_path = os.path.join(root, name)
            if os.listdir(dir_path) == []:
                os.rmdir(dir_path)
        for name in files:
            file_path = os.path.join(root, name)
            if os.path.getsize(file_path) == 0:
                os.remove(file_path)