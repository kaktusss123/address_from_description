class LList:

    class Node:
        def __init__(self, value, prev=None, nxt=None):
            self.value = value
            self.prev = prev
            self.nxt = nxt

    def __init__(self):
        self.head = None
        self.tail = None

    def push_front(self, value):
        if self.head is None:
            self.head = self.tail = Node(value)
        else:
            tmp = Node(value, nxt=self.head)
            self.head = tmp

    def push_back(self, value):
        if self.tail is None:
            self.tail = self.head = Node(value)
        else:
            tmp = Node(value, prev=self.tail)
            self.tail = tmp
