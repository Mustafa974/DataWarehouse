class Ticker(object):
    """
    计数器
    """
    def __init__(self, life: int):
        self._life = life
        self._tick = 0

    @property
    def life(self):
        return self._life

    @property
    def tick(self):
        return self._tick

    @tick.setter
    def tick(self, value):
        if value < 0 or value > self._life:
            raise ValueError('value should be within 0~%d' % self._life)
        self._tick = value

    @life.setter
    def life(self, value):
        if value < 0:
            raise ValueError('value should be greater than 0')
        self._life = value

    def tick_once(self):
        self._tick += 1

    def reset_tick(self):
        self._tick = 0

    def is_alive(self):
        return self._tick <= self._life






