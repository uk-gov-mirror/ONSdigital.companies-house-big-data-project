from os import listdir
from os.path import isfile, join
import cv2

def import_files(directory):
    """
    WHAT DOES IT DO
    
    Arguments:
        directory: source of files of interest
    Returns:
        List of files in a given directory
    Raises:
        None
    """
    return [f for f in listdir(directory) if isfile(join(directory, f))]

def trim_extensions(file_list):
    new_file_list = []
    for file in file_list:
        new_file = ('.').join(file.split('.')[:-1])
        new_file_list.append(new_file)
    return new_file_list

list_of_files = import_files("D:\\companies_house_project_data\\chp_cover_sheet\\")
#print(list_of_files)

file_names = trim_extensions(list_of_files)

def resizeMultipleImages(image_list, scale_factor):
    for image in image_list:
        original_image = cv2.imread("D:\\companies_house_project_data\\chp_cover_sheet\\" + image + ".tif", -1)
        new_width = int(original_image.shape[1] * scale_factor)
        new_height = int(original_image.shape[0] * scale_factor)
        new_dimension = (new_width, new_height)
        resized = cv2.resize(original_image, new_dimension, interpolation = cv2.INTER_AREA)
        cv2.imwrite("D:\\companies_house_project_data\\chp_cover_sheet_resized\\RESIZED-" + image + ".png", resized)

resizeMultipleImages(file_names, 0.3)
