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
        """
        Convert text offset indexes into text snippets.

        Arguments:
            el:         Layout element from the document response from Doc AI
            document:   Document element response from Doc AI
        Returns:
            response:   The text corresponding to that layout element
        Raises:
            None
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
        """
        Returns the vertices of the bounding polygon of a layout element as
        a list of pairs of coordinates.

        Arguments:
            layout_el:  Layout element from the document response from Doc AI
        Returns:
            vertices:   List of pairs of vertices that bound the element
        Raises:
            None
        """
        vertices = []
        for vertex in layout_el.bounding_poly.normalized_vertices:
            vertices.append([vertex.x, vertex.y])
        return vertices

    def parse_document(self, input_uri, token_path, project_id):
        """
        Facilitates sending a request to the Doc AI API and saves the
        response (a 'document') as a class attribute.

        Arguments:
            input_uri:  The gcs location of a pdf to be processed by Doc AI
            token_path: Path to the location of json key for authorisation
            project_id: The gcp project id
        Returns:
            None
        Raises:
            None
        """
        credentials = service_account.Credentials.from_service_account_file(
            token_path)

        client = documentai.DocumentUnderstandingServiceClient(credentials=
                                                               credentials)

        gcs_source = documentai.types.GcsSource(uri=input_uri)

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
        """
        Converts a document response of the API (saved as a class attribute)
        to a DataFrame of individual 'tokens' detected from the document (also
        saved as a class attribute).

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        # Set up a dict to save token data to
        rows = {"value": [],
                "confidence": [],
                "detected_break_type": [],
                "detected_break_count": [],
                "normed_vertices": []}

        # Extract the tokens reference from the document object
        tokens = self.document.pages[0].tokens
        for token in tokens:
            # Get the text for each layout element
            rows["value"].append(
                self.get_text(token.layout, self.document))
            rows["confidence"].append(token.layout.confidence)
            rows["detected_break_type"].append(token.detected_break.type_.name)
            rows["detected_break_count"].append(
                token.detected_break.type_.value)
            rows["normed_vertices"].append(
                self.get_vertices(token.layout))

        df = pd.DataFrame.from_dict(rows)

        # Convert the normed vertices to strings - this just means the rest
        # of the pipeline is compatible if we pass the df in as a csv
        df["normed_vertices"] = df["normed_vertices"].astype("str")
        df["value"] = df["value"].replace('"', "")
        self.token_df = df

