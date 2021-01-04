from google.cloud import documentai_v1beta2 as documentai
from google.oauth2 import service_account
import gcsfs
import pandas as pd

class DocParser:

    def __init__(self):
        self.document = None
        self.token_df = None

    @staticmethod
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

    @staticmethod
    def get_vertices(layout_el):
        vertices = []
        for vertex in layout_el.bounding_poly.normalized_vertices:
            vertices.append([vertex.x, vertex.y])
        return vertices

    def parse_document(self, input, token_path, project_id):
        credentials = service_account.Credentials.from_service_account_file(
            token_path)

        client = documentai.DocumentUnderstandingServiceClient(credentials=
                                                               credentials)

        gcs_source = documentai.types.GcsSource(uri=input)

        # mime_type can be application/pdf, image/tiff,
        # and image/gif, or application/json
        input_config = documentai.types.InputConfig(
            gcs_source=gcs_source, mime_type='application/pdf')

        parent = 'projects/{}/locations/eu'.format(project_id)

        request = documentai.types.ProcessDocumentRequest(
            parent=parent,
            input_config=input_config)

        document = client.process_document(request=request)
        self.document = document

    def tokens_to_df(self):
        rows = {"value": [],
                "confidence": [],
                "detected_break_type": [],
                "detected_break_count": [],
                "normed_vertices": []}

        tokens = self.document.pages[0].tokens
        for token in tokens:
            # db = None
            # try:
            #     db = token.detected_break.type_
            # except:
            # pass
            rows["value"].append(
                self.get_text(token.layout, self.document))
            rows["confidence"].append(token.layout.confidence)
            rows["detected_break_type"].append(token.detected_break.type_.name)
            rows["detected_break_count"].append(
                token.detected_break.type_.value)
            rows["normed_vertices"].append(
                self.get_vertices(token.layout))

        df = pd.DataFrame.from_dict(rows)
        df["normed_vertices"] = df["normed_vertices"].astype("str")
        df["value"] = df["value"].replace('"', "")
        self.token_df = df

