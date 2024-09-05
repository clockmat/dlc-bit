import hashlib
import os
import re
import shutil

import bencodepy
import requests


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


def calulate_torrent_hash(uri: str) -> str | None:
    if uri.startswith("magnet:"):
        return re.search(r"xt=urn:btih:([a-zA-Z0-9]+)", uri).group(1)
    elif uri.startswith("http"):
        torrent_file = requests.get(uri).content
        decoded_torrent = bencodepy.decode(torrent_file)
        torrent_info = bencodepy.encode(decoded_torrent[b"info"])
        return hashlib.sha1(torrent_info).hexdigest()
    else:
        raise NotImplementedError(f"Unsupported URI: {uri}")
