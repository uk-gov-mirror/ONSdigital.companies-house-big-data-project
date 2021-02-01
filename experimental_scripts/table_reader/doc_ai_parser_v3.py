from google.cloud import documentai_v1beta3 as documentai
from google.api_core.client_options import ClientOptions
from google.oauth2 import service_account
import gcsfs
import pandas as pd

class DocParser:
    def __init__(self, fs):
        self.fs = fs
        self.document = None
        self.token_df = None

    def parse_document(self, input_uri, processor_id,
                       token_path, project_id):
        # Instantiates a client
        credentials = service_account.Credentials.from_service_account_file(
            token_path)
        client_options = {"api_endpoint": "eu-documentai.googleapis.com"}
        client = documentai.DocumentProcessorServiceClient(
            credentials=credentials,
            client_options=client_options)

        name = f"projects/{project_id}/locations/eu/processors/{processor_id}"
        with self.fs.open(input_uri, "rb") as image:
            image_content = image.read()

        # Read the file into memory
        document = {"content": image_content, "mime_type": "application/pdf"}

        # Configure the process request
        request = documentai.types.ProcessRequest(name=name,
                                                  document=document,
                                                  skip_human_review=False)

        # Recognizes text entities in the PDF document
        result = client.process_document(request=request)

        document = result.document
        self.document = document


if __name__ == "__main__":
    import requests
    fs = gcsfs.GCSFileSystem("ons-companies-house-dev", token="/home/dylan_purches/Desktop/dev_key.json")
    parser = DocParser(fs)
    parser.parse_document("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/02460543_dormant_bs.pdf",
                          "643a05097d4ab993",
                          "/home/dylan_purches/Desktop/dev_key.json","ons-companies-house-dev")

    # with fs.open("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/02079355_dormant.pdf", "rb") as image:
    #     image_content = image.read()
    # document = {"content": image_content, "mime_type": "application/pdf"}
    # headers = {"Authorization": "Bearer ya29.c.Ko0B7QeJpsX5Ss6CBZX8HxYinStMRm_YTsG94-Tx9KBqlDRz5sMjoozTRYMDQIyXNiVNipwtd9q4vMwlhLXqFzyiz1u9I89PjZK_Jue7qxZKbVXOzmHJHWQBhfR860lTQ1Um6v3f5l0PEmSwK_yvA1HsT4GMvQf3UcI8NPAvJC5H3x8y7gh1utybh3Bhfbv-",
    #            "Content-Type":"application/json; charset=utf-8"}
    #
    # doc = requests.post("https://eu-documentai.googleapis.com/v1beta3/projects/659935619232/locations/eu/processors/643a05097d4ab993:process",
    #                     headers=headers, json=document)
