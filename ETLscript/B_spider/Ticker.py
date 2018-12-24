class Ticker:
    """
    计数器
    """
    def __init__(self, life: int):
        self.life = life
        self.tick = 0

    @property
    def life(self):
        return self.life

    @property
    def tick(self):
        return self.tick

    @tick.setter
    def tick(self, value):
        if value < 0 or value > self.life:
            raise ValueError('value should be within 0~%d' % self.life)
        self.tick = value

    @life.setter
    def life(self, value):
        if value < 0:
            raise ValueError('value should be greater than 0')
        self.life = value

    def tick_once(self):
        self.tick += 1

    def reset_tick(self):
        self.tick = 0

    def is_alive(self):
        return self.tick <= self.life






