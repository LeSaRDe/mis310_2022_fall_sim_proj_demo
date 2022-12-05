

class PriQ:
    """
    This class implements a generic priority queue.
    """
    # Min-Heap or Max-Heap
    #   0: Min-Heap
    #   1: Max-Heap (default)
    m_type = 0

    def insert(self, item):
        """
        Insert an item into the queue.
        :param item: (tuple) The first element of this tuple is the priority which is a numeric.
        :return: (PriQ) The instance of queue.
        """
        # TODO
        pass

    def insert_batch(self, l_item):
        """
        Insert a list of items into the queue.
        :param l_item:
        :return:
        """
        # TODO
        pass

    def del_by_id(self, item_id):
        """
        Delete an item from the queue by its ID.
        :param item_id:
        :return:
        """
        # TODO
        pass

    def extract_top(self):
        """
        Extract-Min for Min-Heap, and Extract-Max for Max-Heap.
        :return: (tuple) The top item of the min/max priority.
        """
        # TODO
        pass

    def change_key(self, idx, new_key):
        """
        Change the key (i.e. the priority value) of an item in the priority queue. This function determines changing the key is `decrease-key` or `increase-key`, and applies different implementations accordingly.
        :param idx: (int) The index of the target item.
        :param new_key: (int) The new key value set to the target item.
        :return: (bool) True - Success. False - Failure.
        """
        # TODO
        pass

    def access_top(self):
        """
        For Max-Heap, this is `max`; and for Min-Heap, this is `min`.
        :return: (tuple or None) Return the item at the root if the queue is not empty. Otherwise, return None.
        """
        # TODO
        pass
