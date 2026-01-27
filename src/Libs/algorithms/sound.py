import winsound
import threading

class Sound:
    def __init__(self):
        self.sounds = {
            'start': '../media/Start.wav',
            'step': '../media/Step.wav'
        }
    
    def signal(self, sound_type='step'):
        winsound.PlaySound(self.sounds[sound_type], winsound.SND_FILENAME)
    
    def async_sound(self, sound_type='step'):
        def _signal():
            winsound.PlaySound(self.sounds[sound_type], winsound.SND_FILENAME)

        thread = threading.Thread(target=_signal())
        thread.start
        return thread