
class Message:
    def __init__(self, content=None, checkpoint=None):
        self.checkpoint = checkpoint
        self.content = content

    def __str__(self):
        return str(self.__dict__)
