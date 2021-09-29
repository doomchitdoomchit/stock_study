from pykrx import stock
import _pickle as cPickle
import gzip
import os


def exist_kickersnamefile(folder_path: str) -> bool:
    """
    해당 폴더에 kickers-name 파일이 있는지 확인
    :param folder_path: str, kickers_name 파일이 있을만한 폴더
    :return: bool
    """
    temp_file_list = os.listdir(folder_path)
    if 'kickers_name.pkl.gz' in temp_file_list:
        return True
    else:
        return False

def check_file(folder_path: str) -> bool:
    """

    :param folder_path:
    :return:
    """

def init_file() -> dict:
    """
    첫 파일 만들기
    :return: dict, kickersname
    """

