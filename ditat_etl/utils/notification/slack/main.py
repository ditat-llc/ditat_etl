import slack

'''
This assumes the token provided and channels are connected.
'''

class Slack:
	def __init__(self, token):
		self.client = slack.WebClient(token=token)

	def send_message(self, channel, text):
		resp  = self.client.chat_postMessage(channel=channel, text=text)
		print(resp)
