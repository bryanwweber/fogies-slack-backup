from datetime import datetime, UTC
from base64 import b64encode
from dataclasses import dataclass
import inspect
import json
from pathlib import Path

import requests
from slack_sdk import WebClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]

# The ID of a sample document.
DOCUMENT_ID = ""


bot_token = ""
user_token = ""
client = WebClient(token=user_token)


@dataclass
class User:
    id: str
    real_name: str
    display_name: str | None

    def __post_init__(self):
        if not self.display_name:
            self.display_name = self.real_name

    @classmethod
    def from_user_id(cls, user_id):
        user = client.users_profile_get(user=user_id)
        params = {
            k: v
            for k, v in user["profile"].items()
            if k in inspect.signature(cls).parameters
        }
        params["id"] = user_id
        return cls(**params)


@dataclass
class Message:
    user: User
    ts: datetime
    thread_ts: str | None
    text: str
    blocks: list
    files: dict | None

    @classmethod
    def from_dict(cls, env):
        params = {
            k: v for k, v in env.items() if k in inspect.signature(cls).parameters
        }
        user_id = params.pop("user")
        params["user"] = User.from_user_id(user_id)
        params["text"] = handle_blocks(params["blocks"])
        params["ts"] = datetime.fromtimestamp(float(params.pop("ts")), tzinfo=UTC)
        if "thread_ts" not in params:
            params["thread_ts"] = None
        if "files" not in params:
            params["files"] = None
        else:
            params["files"] = handle_files(params.pop("files"))
        return cls(**params)


def handle_message(conversation_id, message):
    if message.get("thread_ts"):
        threads = handle_threads(conversation_id, message["thread_ts"])
    else:
        threads = [Message.from_dict(message)]
    return threads


def handle_threads(conversation_id, thread_ts):
    result = client.conversations_replies(channel=conversation_id, ts=thread_ts)
    return [Message.from_dict(mm) for mm in result["messages"]]


def handle_files(files):
    contents = {}
    for ff in files:
        if ff.get("url_private_download"):
            response = requests.get(
                ff["url_private_download"],
                headers={"Authorization": f"Bearer {user_token}"},
                timeout=30,
            )
            contents[ff["name"]] = b64encode(response.content).decode("utf-8")
    return contents


def handle_blocks(blocks):
    text = []
    for block in blocks:
        for element in block["elements"][0]["elements"]:
            if element["type"] == "text":
                text.append(element["text"])
            elif element["type"] == "user":
                user = User.from_user_id(element["user_id"])
                text.append(user.display_name)
            elif element["type"] == "emoji":
                text.append(chr(int(element["unicode"], 16)))

    return "".join(text)


def main():
    if Path("messages.json").exists():
        messages = json.loads(Path("messages.json").read_text())
    general_messages = reversed(messages["C018NNP4560"])
    # result = client.conversations_list()
    # channels = result["channels"]
    # messages = defaultdict(list)
    # for channel in channels:
    #     result = client.conversations_history(channel=channel["id"])
    #     if result["messages"]:
    #         messages[channel["id"]].extend(
    #             asdict(mm)
    #             for mm in handle_message(channel["id"], result["messages"][0])
    #         )
    # Path("messages.json").write_text(json.dumps(messages))
    creds = service_account.Credentials.from_service_account_file(
        filename="sa-creds.json", scopes=SCOPES
    )

    try:
        service = build("drive", "v3", credentials=creds)
        # all_files = service.files().list().execute()
        docs = build("docs", "v1", credentials=creds)

        # Retrieve the documents contents from the Docs service.
        # document = service.files().list().execute()
        for message in general_messages:
            dd = docs.documents().get(documentId=DOCUMENT_ID).execute()
            for cc in dd["body"]["content"]:
                if "table" not in cc:
                    continue
                n_rows = cc["table"]["rows"]
                break
            docs.documents().batchUpdate(
                documentId=DOCUMENT_ID,
                body={
                    "requests": [
                        {
                            "insertTableRow": {
                                "tableCellLocation": {
                                    "tableStartLocation": {
                                        "index": 2,
                                        "tabId": "",
                                    },
                                    "rowIndex": n_rows - 1,
                                    "columnIndex": 0,
                                },
                                "insertBelow": "true",
                            }
                        }
                    ]
                },
            ).execute()
            dd = docs.documents().get(documentId=DOCUMENT_ID).execute()
            cell_starts = []
            for cc in dd["body"]["content"]:
                if "table" not in cc:
                    continue
                table_row = cc["table"]["tableRows"][-1]
                for cell in table_row["tableCells"]:
                    for para in cell["content"]:
                        cell_starts.append(para["startIndex"])
                break
            text_requests = []
            # TODO: Add file/photo inserting
            text_requests.append(
                {
                    "insertText": {
                        "location": {"index": cell_starts[-2]},
                        "text": message["text"],
                    }
                }
            )
            text_requests.append(
                {
                    "insertText": {
                        "location": {"index": cell_starts[-3]},
                        "text": datetime.fromtimestamp(
                            float(message["ts"]), tz=UTC
                        ).isoformat(),
                    }
                }
            )
            text_requests.append(
                {
                    "insertText": {
                        "location": {"index": cell_starts[-4]},
                        "text": message["user"]["display_name"],
                    }
                }
            )
            docs.documents().batchUpdate(
                documentId=DOCUMENT_ID, body={"requests": text_requests}
            ).execute()
        # rr = (
        #     docs.documents()
        #     .batchUpdate(
        #         documentId=DOCUMENT_ID,
        #         body={
        #             "requests": [
        #                 # {
        #                 #     "insertTable": {
        #                 #         "columns": 5,
        #                 #         "rows": 2,
        #                 #         "endOfSegmentLocation": {"segmentId": ""},
        #                 #     }
        #                 # }
        #                 # This has to go from largest to smallest index because each
        #                 # insertion affects the index of the next.
        #                 {"insertText": {"location": {"index": 7}, "text": "Hello"}},
        #                 {"insertText": {"location": {"index": 5}, "text": "Hello"}},
        #             ]
        #         },
        #     )
        #     .execute()
        # )

        # pprint(dd)
        # print(rr)
    except HttpError as err:
        print(err)


if __name__ == "__main__":
    main()
