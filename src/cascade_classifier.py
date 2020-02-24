# import the necessary packages
import argparse
import sys
import cv2
import numpy as np

#
chp_stamps_cascade_classifier = "/Users/spot/Desktop/chp_stamps_cascade_classifier.xml"
chp_signatures_cascade_classifier = "some dir here"

# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--input_imgs", required = True,
	help = "path to the input image")
ap.add_argument("-c", "--cascade",
	default = chp_stamps_cascade_classifier,
	help = "path to companies house stamp detector.")
ap.add_argument("-o", "--output_imgs", required = True,
	help = "path to classifier output")
args = vars(ap.parse_args())

# load the input image and convert it to grayscale
image = cv2.imread(args["input_imgs"])
output_path = args["output_imgs"]
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# load the cat detector Haar cascade, then detect cat faces in the input image
detector = cv2.CascadeClassifier(args["cascade"])
rects = detector.detectMultiScale(gray, scaleFactor = 2.0, minNeighbors = 10, minSize = (60, 30))

print("[INFO] Found {0} companies house stamps".format(len(rects)))

# loop over the cat faces and draw a rectangle surrounding each
for (i, (x, y, w, h)) in enumerate(rects):
	cv2.rectangle(image, (x, y), (x + w, y + h), (0, 0, 255), 2)
	cv2.putText(image, "CH Stamp #{}".format(i + 1), (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 255), 2)
	roi_colour = image[y:y + h, x:x + w]
	print("[INFO] Object classified. Saving locally")
	cv2.imwrite(output_path + str(w) + str(h) + "_ch_stamps.png", roi_colour)

status = cv2.imwrite(output_path + "test.png", image)
print("[INFO] Output from the classifier written to filesystem in: " + output_path, "[STATUS]: " + str(status))

# show the detected cat faces
#cv2.imshow("Companies House Stamps", image)
#cv2.waitKey(0)
def shou_classifier_output():
    '''
    '''
    pass

def show_processed_output():
    '''
    '''
    pass
