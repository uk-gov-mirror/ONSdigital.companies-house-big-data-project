from os import listdir
from os.path import isfile, join
import time
import argparse
import sys
import cv2

import cst_data_processing
import cst_classifier
from cst_classifier import classifier_input, imgs_to_grey, classifier, classifier_output_count, classifier_output, classifier_process

def main():
    
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