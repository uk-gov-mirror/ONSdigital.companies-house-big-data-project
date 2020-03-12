# import the necessary packages
from os import listdir
from os.path import isfile, join
import glob
import argparse
import sys
import cv2
import numpy
import time

def imgs_to_grey(path):
    '''
    '''
    if path is None:
        raise ValueError("Specify a path to an image.")
    if type(path) not in [""]:
        raise TypeError("The value of the path argument must be a of type string")
    return cv2.cvtColor(path, cv2.COLOR_BGR2GRAY)

def classifier_input(path):
    '''
    '''
    if path is None:
        raise ValueError("Specify a path to an image.")
    if type(path) not in [""]:
        raise TypeError("The value of the path argument must be a of type string")
    return cv2.imread(path)
    
def classifier(detector, imgs, scale_factor, min_neighbors, w, h):
    '''
    '''
    return detector.detectMultiScale(imgs, scale_factor, min_neighbors, minSize = (w, h))

def classifier_output_show(title, imgs):
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
    status = cv2.imwrite(path, image)
    print("[INFO] Output from the classifier written to filesystem in: " + path, "[STATUS]: " + str(status))
    
def main():
    '''

    EXE on Mac " python3 cascade_classifier.py -d ../data/input/test_img_04.tif -c ../data/cascade_files/chp_stamp_cascade_classifier_20200224.xml -s 1.2 -m 10 -x 60 -y 30 -o ../data/output/classifier_output/ -p ../data/output/processed_images -v False/ "

    EXE on a Windows machine " python cascade_classifier.py -d ..\data\input\test_img_04.tif -c ..\data\cascade_files\chp_stamp_cascade_classifier_20200224.xml -s 1.2 -m 10 -x 60 -y 30 -o ..\data\output\classifier_output\ -p ..\data\output\processed_images\ -v False"
    
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
    ap.add_argument("-v", "--show_classifier_output", required = False,
        help = "display the output of the classifier to the user.")
    args = vars(ap.parse_args())

    detector = cv2.CascadeClassifier(args["cascade_file"])

    # load the input image and convert it to grayscale
    for image in listdir(args["input_imgs"]):
        img = classifier_input(join(args["input_imgs"], image))
        grey = imgs_to_grey(img)

        rects = classifier(detector,
                           grey,
                           float(args["scale_factor"]),
                           int(args["min_neighbors"]),
                           int(args["cascade_width"]),
                           int(args["cascade_height"]))

        print("\n[INFO] Found " + str(classifier_output_count(rects)) + " companies house stamps.")

        #
        classifier_output(rects, img, args["classifier_output_dir"])

        #
        classifier_process(img, join(args["processed_images_dir"], image))

        #
        if args["show_classifier_output"] == "True":
            classifier_output_show(image, img)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
        else:
            pass

if __name__ == "__main__":

    process_start = time.time()
    
    main()
    
    print("-"*50)
    print("Process Complete")
    print("The time taken to process an image is: ", "{}".format((time.time() - process_start)/60, 2), " minutes")
