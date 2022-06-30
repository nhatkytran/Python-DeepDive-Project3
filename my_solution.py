from datetime import datetime
from collections import namedtuple

# Iterator solution


class TicketIter:
    def __init__(self):
        self.file = open('nyc_parking_tickets_extract.csv')
        self.Ticket = namedtuple('Ticket', self.transform_row(
            next(self.file), self.transform_name))

    def transform_row(self, row, callback=None):
        result = row.strip('\n').split(',')
        if callback is None:
            return result
        return callback(result)

    def transform_name(self, names):
        return [('_').join(name.lower().split()) for name in names]

    def transform_type(self, row):
        def transform_type_helper(item):
            try:
                return int(item)
            except Exception:
                try:
                    return datetime.strptime(item, '%m/%d/%Y').date()
                except:
                    return item
        return [transform_type_helper(item) for item in row]

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.Ticket(*self.transform_row(next(self.file), self.transform_type))
        except StopIteration:
            self.file.close()
            raise StopIteration
# ticket = TicketIter()
# print(next(ticket))


# Generator solution //////////


def gen_ticket():
    def transform_row(row, callback=None):
        result = row.strip('\n').split(',')
        if callback is None:
            return result
        return callback(result)

    def transform_name(names):
        return [('_').join(name.lower().split()) for name in names]

    def transform_type(data):
        def transform_type_helper(item):
            try:
                return int(item)
            except Exception:
                try:
                    return datetime.strptime(item, '%m/%d/%Y').date()
                except Exception:
                    return item
        return [transform_type_helper(item) for item in data]

    with open('nyc_parking_tickets_extract.csv') as f:
        Ticket = namedtuple('Ticket', transform_row(next(f), transform_name))
        while True:
            try:
                result = transform_row(next(f), transform_type)
                yield Ticket(*result)
            except StopIteration:
                return None
# ticket = gen_ticket()
# print(next(ticket))
