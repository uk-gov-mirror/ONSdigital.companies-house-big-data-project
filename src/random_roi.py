from os import listdir
from os.path import isfile, join
import cv2
import random

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

no_extension_image_list = import_files("D:\\companies_house_project_data\\chp_negative_data_samples_resized\\")
print(no_extension_image_list)



def check_image_list(list_of_images):
	output_list = []
	for image_file in list_of_images[:5]:
		output_list.append(image_file)
	return output_list

#check_image_list(no_extension_image_list)



def create_random_ROI_from_list(list_of_images, ROI_count, rect_width, rect_height, output_path):
	# D:\companies_house_project_data\chp_negative_data_samples_resized
    for image_file in list_of_images:
        image = cv2.imread("D:\\companies_house_project_data\\chp_negative_data_samples_resized\\" + str(image_file), -1)
        #print(image.shape)
        image_height, image_width = image.shape[:-1]

        for i in range(ROI_count):
            left_coord = random.randrange(0, image_width - rect_width + 1, 1)
            top_coord = random.randrange(0, image_height - rect_height + 1, 1)
            right_coord = left_coord + rect_width
            bottom_coord = top_coord + rect_height
            region_of_interest = image[top_coord:bottom_coord, left_coord:right_coord]
            cv2.imwrite(
                output_path
                    + str(image_file)
                    + "___("
                    + str(left_coord)
                    + str(",")
                    + str(top_coord)
                    + ").png"
                    + "_w="
                    + str(rect_width)
                    + "_h="
                    + str(rect_height)
                    + "_dtype=COLOUR" # if image_file.shape[2] != 1: "COLOUR" else: "GREYSCALE"
                    + ".png",
                region_of_interest
            )

create_random_ROI_from_list(no_extension_image_list, 105, 100, 50, "D:\\companies_house_project_data\\chp_negatives_for_training\\")