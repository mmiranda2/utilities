import pyaudio
import wave
import random
import datetime
import os


CHUNK = 1024
FORMAT = pyaudio.paInt16
WIDTH = 2
CHANNELS = 2
RATE = 44100 // 4
RECORD_SECONDS = 5
WAVE_OUTPUT_DIR_1 = '/Users/test/Library/Mobile Documents/com~apple~CloudDocs/output/output/'
WAVE_OUTPUT_DIR_2 = '/Users/test/Library/Mobile\ Documents/com~apple~CloudDocs/output/output/'


def get_stream():
    aux = pyaudio.PyAudio()
    stream = aux.open(format=aux.get_format_from_width(WIDTH),
                    channels=CHANNELS,
                    rate=RATE,
                    input=True,
                    output=True,
                    frames_per_buffer=CHUNK)
    return aux, stream


def append_chunk(stream, frames):
    data = stream.read(CHUNK)  #read audio stream
    frames.append(data)


def flush_chunk(frames, wavfile):
    wavfile.writeframes(b''.join(frames))
    wavfile.close()


def stamp():
    return str(datetime.datetime.utcnow().timestamp()).replace('.', '_')[:-5]


def get_wavfile(aux, filename):
    wav_filepath = WAVE_OUTPUT_DIR_1 + filename
    wf = wave.open(wav_filepath, 'wb')
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(aux.get_sample_size(FORMAT))
    wf.setframerate(RATE)

    return wf


def main():
    aux, stream = get_stream()
    filename = 'output_{}.wav'.format(stamp())
    wf = get_wavfile(aux, filename)
    try:
        frames = [stream.read(CHUNK) for i in range(0, int(RATE / CHUNK * RECORD_SECONDS))]
        wf.writeframes(b''.join(frames))
    except Exception as e:
        print(e)
        print('\n', str(e))
    finally:
        stream.stop_stream()
        stream.close()
        aux.terminate()
        wf.close()
        os.system('gzip {}'.format(WAVE_OUTPUT_DIR_2 + filename))


if __name__ == '__main__':
    counter = 0
    try:
        while True:
            main()
            counter += 1
    except Exception as e:
        print(e)
        print('\n')
        print(str(e))
        print('\n')
        print('Opened, written, closed, and done :: {} number of loops'.format(str(counter)))
