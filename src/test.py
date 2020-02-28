import os
import cv2
path_of_images = r"E:\companies_house_project\repos\companies_house_project\data\input"
list_of_images = os.listdir(path_of_images)
for image in list_of_images:
    img = cv2.imread(os.path.join(path_of_images, image))
