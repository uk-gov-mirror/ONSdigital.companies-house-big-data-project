import pandas as pd
from PIL import Image, ImageDraw
from pdf2image import convert_from_path

"""
To install poppler:
    sudo apt-get install -y poppler-utils
"""

class PDFAnnotator():
    def __init__(self, in_path, out_path = False):
        self.in_path = in_path
        if not out_path: 
            self.out_path = (in_path.split("/")[-1]).split(".")[0]
        else:
            self.out_path = out_path

    def pdf_to_png(self):
        pages = convert_from_path(self.in_path, 500)
        if len(pages) == 1:
            pages[0].save(self.out_path + ".jpg", "JPEG")
        else:
            for page, n in enumerate(pages):
                page[0].save(self.out_path +"_"+n+ ".jpg", "JPEG")
    
    def annotate_boxes_pdf(self,path, shapes):
        img = Image.open(path)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)

        for v in shapes:
            xy = eval(v)
            xy = [(x[0]*width, x[1]*height) for x in xy]
            draw.polygon(xy, outline = "lime",fill = "wheat")

        img3 = Image.blend(img, img2, 0.4)
        img3.save(self.out_path + "_annotated.jpg")

    def annotate_lines_pdf(self, path, lines):
        img = Image.open(path)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)

        for line in lines:
            draw.line((line*width, 0, line*width, height), fill = "red", width = 5)

        img3 = Image.blend(img, img2, 0.4)
        img3.save(self.out_path + "_annotated.jpg")
            
            

