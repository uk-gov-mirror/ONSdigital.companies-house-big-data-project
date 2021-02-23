from google.cloud import documentai_v1beta2 as documentai
from google.cloud import storage
import gcsfs
import os
import csv
import shutil
import pandas as pd 

#test_df = pd.read_csv('gs://ons-companies-house-dev-scraped-pdf-data/pdf_active/01961626 active.pdf')

class Document_AI_output:
    """
    Class that takes a bucket input and uses the Document AI API to preform OCR on the PDF content of the bucket and exports to
    a desired bucket location
    
    ENSURE YOU HAVE RUN auth.sh and have a 'key.json' file in your local directory
    """
    #define authentication
    fs = gcsfs.GCSFileSystem("ons-companies-house-dev", token="key.json")

    def upload_file(bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print('File "{}" uploaded as "{}" to the bucket "{}".'.format(source_file_name, destination_blob_name, bucket_name))

    def list_bucket_paths(bucket_name, prefix, delimiter=None):

        storage_client = storage.Client()

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(
            bucket_name, prefix=prefix, delimiter=delimiter
        )
        return ["gs://"+bucket_name+"/"+ x.name for x in blobs]


    def get_text(el, document):
        """Convert text offset indexes into text snippets.
        """
        response = ''
        # If a text segment spans several lines, it will
        # be stored in different text segments.
        for segment in el.text_anchor.text_segments:
            start_index = segment.start_index
            end_index = segment.end_index
            response += document.text[start_index:end_index]
        return response

    def get_vertices(layout_el):
        vertices = []
        for vertex in layout_el.bounding_poly.normalized_vertices:
            vertices.append([vertex.x, vertex.y])
        return vertices

    def parse_table(project_id='ons-companies-house-dev',
            input_uri='gs://ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/04391694_active_bs.pdf', print_stuff = False):
        """Parse a form"""

        client = documentai.DocumentUnderstandingServiceClient()

        gcs_source = documentai.types.GcsSource(uri=input_uri)

        # mime_type can be application/pdf, image/tiff,
        # and image/gif, or application/json
        input_config = documentai.types.InputConfig(
            gcs_source=gcs_source, mime_type='application/pdf')

        # Improve table parsing results by providing bounding boxes
        # specifying where the box appears in the document (optional)
        table_bound_hints = [
            documentai.types.TableBoundHint(
                page_number=1,
                bounding_box=documentai.types.BoundingPoly(
                    # Define a polygon around tables to detect
                    # Each vertice coordinate must be a number between 0 and 1
                    normalized_vertices=[
                        # Top left
                        documentai.types.geometry.NormalizedVertex(
                            x=0,
                            y=0
                        ),
                        # Top right
                        documentai.types.geometry.NormalizedVertex(
                            x=1,
                            y=0
                        ),
                        # Bottom right
                        documentai.types.geometry.NormalizedVertex(
                            x=1,
                            y=1
                        ),
                        # Bottom left
                        documentai.types.geometry.NormalizedVertex(
                            x=0,
                            y=1
                        )
                    ]
                )
            )
        ]

        # Setting enabled=True enables form extraction
        table_extraction_params = documentai.types.TableExtractionParams(
            enabled=True, table_bound_hints=table_bound_hints,model_version = "builtin/latest",
            header_hints = ["At 31 December\n", "2019\n", "$ million2018\n"])

        # Location can be 'us' or 'eu'
        parent = 'projects/{}/locations/us'.format(project_id)
        request = documentai.types.ProcessDocumentRequest(
            parent=parent,
            input_config=input_config,
            table_extraction_params=table_extraction_params)

        document = client.process_document(request=request)
        return(document)

    def table_to_df(document, table):
        """
        document = return value of parse_table
        table = table object (document.pages[0].tables[0])
        """
        cols = []
        if len(table.header_rows) == 1:
            for head_cell in table.header_rows[0].cells:
                cols.append(Document_AI_output.get_text(head_cell.layout, document))

        rows = {"scraped_col":[],
                "value":[],
                "confidence":[],
                "col_span":[],
                "row_span":[],
                "normed_vertices":[]}
        
        for row in table.body_rows:
            for i, cell in enumerate(row.cells):
                rows["scraped_col"].append(cols[i])
                rows["value"].append(Document_AI_output.get_text(cell.layout, document))
                rows["confidence"].append(cell.layout.confidence)
                rows["col_span"].append(cell.col_span)
                rows["row_span"].append(cell.row_span)
                rows["normed_vertices"].append(Document_AI_output.get_vertices(cell.layout))

        df = pd.DataFrame.from_dict(rows)
        df["value"] = df["value"].replace('"', "")
        df["scraped_col"] = df["scraped_col"].replace('"', "")
        return df

    def tokens_to_df(document, tokens):
        rows = {"value":[],
                "confidence":[],
                "detected_break_type":[],
                "detected_break_count":[],
                "normed_vertices":[]}
        
        for token in tokens:
            # db = None
            # try:
            #     db = token.detected_break.type_
            # except:
            # pass
            rows["value"].append(Document_AI_output.get_text(token.layout, document))
            rows["confidence"].append(token.layout.confidence)
            rows["detected_break_type"].append(token.detected_break.type_.name)
            rows["detected_break_count"].append(token.detected_break.type_.value)
            rows["normed_vertices"].append(Document_AI_output.get_vertices(token.layout))

        df = pd.DataFrame.from_dict(rows)
        df["value"] = df["value"].replace('"', "")
        return df

    def get_line_coordinates(doc):
        n = 0
        enter = input("Continue?")
        while (enter != "q") and n < len(doc.pages[0].lines):
            print(Document_AI_output.get_text(doc.pages[0].lines[n].layout, doc),)
            print(Document_AI_output.get_vertices(doc.pages[0].lines[n].layout)) 
            n+=1
            enter = input("Continue?")

    def get_block_coordinates(doc):
        n = 0
        enter = input("Continue?")
        while (enter != "q") and n < len(doc.pages[0].blocks):
            print(Document_AI_output.get_text(doc.pages[0].blocks[n].layout, doc),)
            print(Document_AI_output.get_vertices(doc.pages[0].blocks[n].layout)) 
            n+=1
            enter = input("Continue?")

    def get_token_coordinates(doc):
        n = 0
        enter = input("Continue?")
        while (enter != "q") and n < len(doc.pages[0].tokens):
            print(Document_AI_output.get_text(doc.pages[0].tokens[n].layout, doc),)
            print(Document_AI_output.get_vertices(doc.pages[0].tokens[n].layout)) 
            n+=1
            enter = input("Continue?")


    def upload_dfs_from_file(bucket_input, bucket_output, elements = "tokens"):
        '''
        Summary function that takes a bucket directory as input and passes each PDF stored within to
        the DOcument AI to be processed. The output can either be in table or token format (elements =).
        The outputs are then converted into DataFrames and exported to the output bucket.

        Parameters:
            bucket_input = url of the GCP bucket the PDf's are stored in
                           *needs to start with gs://{bucket_address}
            
            bucket_output = url of the GCP bucket you would liek to save the output df's 

            elements = string of either 'tokens' or '
        '''
        fs = Document_AI_output.fs

        # Create list of all balance sheets in bucket
        sheets = fs.ls(bucket_input)
        print(sheets)
        # Create a df for each balance sheet and upload it
        for sheet in sheets:
            if 'gs://'+ sheet != bucket_input:
                doc = Document_AI_output.parse_table(input_uri='gs://'+sheet)
                if elements == "tokens":
                    df = Document_AI_output.tokens_to_df(doc, doc.pages[0].tokens)
                elif elements == "tables":
                    df = Document_AI_output.table_to_df(doc, doc.pages[0].tables[0])
                # Extract a relevant file name
                csv_name = (sheet.split("/")[-1]).split(".")[0]

                # Convert file to csv and save in a bucket
                df.to_csv("gs://" + bucket_output+csv_name+"_"+elements+".csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

                # Update metadata of saved file
                fs.setxattrs(bucket_output+csv_name+"_tokens.csv", content_type = "text/csv")

#test on dormant companies
#Document_AI_output.upload_dfs_from_file("gs://ons-companies-house-dev-scraped-pdf-data/pdf_dormant/bs/","ons-companies-house-dev-parsed-pdf-data/")