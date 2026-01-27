
class PrograsBar:
    def __init__(self, set_value=0, max=100, box = '▬', space = ' ', boards = r'||', SIZE=100):
        self._SIZE = SIZE
        self.current = set_value
        self.max = max
        self.box = box
        self.space = space
        self.boards = boards
    
    
    def show(self, add: int | float = 0):
        self.current += add

        _progress = self.current / self.max
        print(
            "\r{}{}{}{} {} %\t({} of {})\t".format(
                self.boards[0] if len(self.boards) == 2 else '',
                self.box * int(_progress * self._SIZE),
                self.space * (self._SIZE - int(_progress * self._SIZE)),
                self.boards[1] if len(self.boards) == 2 else '',
                round(_progress * 100, 2),
                self.current,
                self.max
            ), end='', flush=True
        )
        if self.current >= self.max:
            print()