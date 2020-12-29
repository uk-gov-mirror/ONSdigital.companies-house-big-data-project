import pandas as pd
import gcsfs
from PIL import Image, ImageDraw
from io import BytesIO
from pdf2image import convert_from_path, convert_from_bytes


"""
To install poppler:
    sudo apt-get install -y poppler-utils
"""

fs = gcsfs.GCSFileSystem("ons-companies-house-dev")

class PDFAnnotator:
    def __init__(self, in_path, out_path=False, gcp=False):
        self.in_path = in_path
        self.gcp = gcp
        self.bs_image = None
        if not out_path: 
            self.out_path = (in_path.split("/")[-1]).split(".")[0]
        else:
            self.out_path = out_path

    def pdf_to_png(self):
        if not self.gcp:
            pages = convert_from_path(self.in_path, 500)
            if len(pages) == 1:
                pages[0].save(self.out_path + ".jpg", "JPEG")
            else:
                for page, n in enumerate(pages):
                    page[0].save(self.out_path +"_"+n+ ".jpg", "JPEG")
        elif self.gcp:
            with fs.open(self.in_path, 'rb') as f:
                pages = convert_from_bytes(f.read(), 500)
            if len(pages) == 1:
                byteio = BytesIO()
                pages[0].save(byteio,'JPEG')
                self.bs_image = byteio
            else:
                print("Balance sheet must be one page only")
    
    def annotate_boxes_pdf(self,path, shapes):
        img = Image.open(self.bs_image)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)

        for v in shapes:
            xy = eval(v)
            xy = [(x[0]*width, x[1]*height) for x in xy]
            draw.polygon(xy, outline = "lime",fill = "wheat")

        img3 = Image.blend(img, img2, 0.4)
        with fs.open(path, 'wb') as f:
            byteio = BytesIO()
            img3.save(byteio, 'JPEG')
            f.write(byteio.getvalue())

    def annotate_lines_pdf(self, path, lines):
        img = Image.open(self.bs_image)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)

        for line in lines:
            draw.line((line*width, 0, line*width, height), fill="red", width=5)

        img3 = Image.blend(img, img2, 0.4)
        with fs.open(path, 'wb') as f:
            byteio = BytesIO()
            img3.save(byteio, 'JPEG')
            f.write(byteio.getvalue())
            
            

