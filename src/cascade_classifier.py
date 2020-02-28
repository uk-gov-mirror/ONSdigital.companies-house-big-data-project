# import the necessary packages
from os import listdir
from os.path import isfile, join
import glob
import argparse
import sys
import cv2
import numpy
import time
from skimage.io import imread_collection

def imgs_to_grey(image):
    '''
    '''
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

def classifier_input(path):
    '''
    cv_img = []
    for img in path:
        n = cv2.imread(img, cv2.IMREAD_GRAYSCALE)
        cv_img.append(n)
		
	import os
import cv2
path_of_images = r"E:\companies_house_project\repos\companies_house_project\data\input"
list_of_images = os.listdir(path_of_images)
for image in list_of_images:
    img = cv2.imread(os.path.join(path_of_images, image))
    '''
#    return cv2.imread(path)
	return imread_collection(path)

def classifier(detector, imgs, scale_factor, min_neighbors, w, h):
    '''
    '''
    return detector.detectMultiScale(imgs, scale_factor, min_neighbors, minSize = (w, h))

def classifier_outputdd(title, imgs):
    '''
    '''
    return cv2.imshow(title, imgs)

def classifier_output_count(imgs):
    '''
    '''
    return len(imgs)

def classifier_output(rects, image, path):
    '''
    '''
    for (i, (x, y, w, h)) in enumerate(rects):
        cv2.rectangle(image, (x, y), (x + w, y + h), (0, 0, 255), 2)
        cv2.putText(image, "CH Stamp #{}".format(i + 1), (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 255), 2)
        roi_colour = image[y:y + h, x:x + w]
        print("[INFO] Object classified. Saving locally")
        cv2.imwrite(path + str(w) + str(h) + "_ch_stamps.png", roi_colour)
        
def classifier_process(image, path):
    status = cv2.imwrite(path + "test.png", image)
    print("[INFO] Output from the classifier written to filesystem in: " + path, "[STATUS]: " + str(status))
    
def main():
    '''
	
    EXE on Mac " python3 cascade_classifier.py -d ../data/input/test_img_04.tif -c ../data/cascade_files/chp_stamp_cascade_classifier_20200224.xml -s 1.2 -m 10 -x 60 -y 30 -o ../data/output/classifier_output/ -p ../data/output/processed_images/ "
 
	EXE on a Windows machine " python cascade_classifier.py -d ..\data\input\test_img_04.tif -c ..\data\cascade_files\chp_stamp_cascade_classifier_20200224.xml -s 1.2 -m 10 -x 60 -y 30 -o ..\data\output\classifier_output\ -p ..\data\output\processed_images\ "
    
    '''
    
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--input_imgs", required = True,
        help = "path to a directory containing images to be used as input data")
    ap.add_argument("-c", "--cascade_file", required = True,
        help = "path to cascade classifier.")
    ap.add_argument("-s", "--scale_factor", required = True,
        help = "cascade classifier scale factor.")
    ap.add_argument("-m", "--min_neighbors", required = True,
        help = "cascade classifier min neighbors.")
    ap.add_argument("-x", "--cascade_width", required = True,
        help = "cascade classifier starting sample width.")
    ap.add_argument("-y", "--cascade_height", required = True,
        help = "cascade classifier starting sample height.")
    ap.add_argument("-o", "--classifier_output_dir", required = True,
        help = "path to classifier output.")
    ap.add_argument("-p", "--processed_images_dir", required = True,
        help = "path to images processed by the classifier.")
    args = vars(ap.parse_args())

    # load the input image and convert it to grayscale
    image = classifier_input(args["input_imgs"])
    gray = imgs_to_grey(image)

    #
    detector = cv2.CascadeClassifier(args["cascade_file"])
    rects = classifier(detector,
                       gray,
                       float(args["scale_factor"]),
                       int(args["min_neighbors"]),
                       int(args["cascade_width"]),
                       int(args["cascade_height"]))

    print("[INFO] Found " + str(classifier_output_count(rects)) + " companies house stamps.\n")

    #
    classifier_output(rects, image, args["classifier_output_dir"])

    #
    classifier_process(image, args["processed_images_dir"])

#    cv2.waitKey(0)

if __name__ == "__main__":

    process_start = time.time()
    
    main()
    
    print("-"*50)
    print("Process Complete")
    print("The time taken to process an image is : ", "{}".format((time.time() - process_start)/60, 2), " minutes")
