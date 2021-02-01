import pandas as pd
import gcsfs
from PIL import Image, ImageDraw
from io import BytesIO
from pdf2image import convert_from_path, convert_from_bytes

from table_fitter import TableFitter


"""
To install poppler:
    sudo apt-get install -y poppler-utils
"""


class PDFAnnotator:

    def __init__(self, in_path, out_path=False, gcp=False):
        self.in_path = in_path
        self.gcp = gcp
        self.bs_image = None
        self.fs = gcsfs.GCSFileSystem("ons-companies-house-dev")

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
            with self.fs.open(self.in_path, 'rb') as f:
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
        with self.fs.open(path, 'wb') as f:
            byteio = BytesIO()
            img3.save(byteio, 'JPEG')
            f.write(byteio.getvalue())
        self.fs.setxattrs(path, content_type = "image/jpg")

    def annotate_lines_pdf(self, path, lines):
        img = Image.open(self.bs_image)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)

        for line in lines:
            draw.line((line*width, 0, line*width, height), fill="red", width=5)

        img3 = Image.blend(img, img2, 0.4)
        with self.fs.open(path, 'wb') as f:
            byteio = BytesIO()
            img3.save(byteio, 'JPEG')
            f.write(byteio.getvalue())
        self.fs.setxattrs(path, content_type="image/jpg")

    def annotate_table(self, path, table_data):
        img = Image.open(self.bs_image)
        width, height = img.size

        img2 = img.copy()
        draw = ImageDraw.Draw(img2)
        line_num = table_data.data.reset_index().loc[0,"line_num"]
        while line_num <= max(table_data.data["line_num"]):
            lines_df = table_data.data[table_data.data["line_num"] == line_num].reset_index()
            y_coord = eval(lines_df.loc[0, "normed_vertices"])[3][1]
            draw.line((0, y_coord * height, width, y_coord * height),
                      fill="blue", width=5)
            line_num += 1

        for i in range(len(table_data.columns)):
            shapes = table_data.data.loc[table_data.columns[i],
                                         "normed_vertices"]
            for v in shapes:
                xy = eval(v)
                xy = [(x[0] * width, x[1] * height) for x in xy]
                draw.polygon(xy, outline="lime", fill="wheat")
            fitter = TableFitter(table_data.data)
            x_coord = (fitter.find_alignment(table_data.data, table_data.columns[i]))["median_points"][2]
            draw.line((x_coord*width, 0, x_coord*width, height), fill="red", width=5)

        img3 = Image.blend(img, img2, 0.4)
        with self.fs.open(path, 'wb') as f:
            byteio = BytesIO()
            img3.save(byteio, 'JPEG')
            f.write(byteio.getvalue())
        self.fs.setxattrs(path, content_type = "image/jpg")


            
            

