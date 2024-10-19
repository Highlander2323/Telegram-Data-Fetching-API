import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient
from elasticsearch import Elasticsearch

es_user = 'elastic'
es_pass = 'uKL_nLBm-bTfoqm5xx34'
es_scheme = 'https'
es_port = 9200
es_host = 'localhost'

# Replace the values with your own API ID, API hash and phone number
# enter your api id from telegram website
api_id = 'id'
api_hash = 'hash'
# enter your api hash form telegram api website
phone_number = 'number'
# enter your pone number on this formate

groups_file = open('telegram_channels.txt', 'r')
# enter your channel group id -100 after this digit

# Set the time range to get messages from
start_time = datetime.now() - timedelta(hours=24)
flag = 0
groups = groups_file.readlines()


def greet():
    while True:
        days = int(input('Choose the number of days that represent the period within which data will be'
              ' collected (e.g. last 5 days) (Max. 14 days)\n>>>'))
        if days > 0 and days < 15:
            break
    return days


async def get_group_messages(days):
    # Create a Telegram client with the specified API ID, API hash and phone number
    client = TelegramClient('session_name', int(api_id), api_hash)
    await client.connect()

    # Check if the user is already authorized, otherwise prompt the user to authorize the client
    if not await client.is_user_authorized():
        await client.send_code_request(phone_number)
        await client.sign_in(phone_number, input('Enter the code: '))

    data = []

    # Get the ID of the specified group
    for index in range(len(groups)):
        group = await client.get_entity(groups[index])
        date_today = datetime.now()
        lower_bound = date_today - timedelta(days=days)

        # below commented code is used for specified time range
        async for message in client.iter_messages(group, min_id=1):
            if str(message.date) < str(lower_bound):
                break

            message_reactions = message.reactions
            reactions = []
            if message_reactions:
                reaction_counts = message_reactions.results

                for reaction_count in reaction_counts:
                    emoji = reaction_count.reaction.emoticon
                    count = reaction_count.count
                    reactions.append({'emoji': emoji, 'count': count})

            data_object = {"channel": group.title, "date": message.date, "text": message.text,
                           "views": message.views,
                           "reactions": reactions}

            data.append(data_object)

    return data


def index_messages_to_elasticsearch(messages):
    es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}], basic_auth=(es_user, es_pass))

    for message in messages:
        es.index(index='telegram', body=message)


async def main():
    days = greet()
    messages = await get_group_messages(days)
    index_messages_to_elasticsearch(messages)

# Run the main function
asyncio.run(main())
