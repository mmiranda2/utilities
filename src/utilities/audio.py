import os
import gzip
import shutil
import random
import datetime
import pyaudio
import wave
from pydub import AudioSegment


CHUNK = 1024
FORMAT = pyaudio.paInt16
WIDTH = 2
CHANNELS = 2
RATE = 44100 // 2
TIME_PERIOD = 5
WAVE_OUTPUT_DIR_1 = '/Users/test/Library/Mobile Documents/com~apple~CloudDocs/output/output/'
WAVE_OUTPUT_DIR_2 = '/Users/test/Library/Mobile\ Documents/com~apple~CloudDocs/output/output/'


def stamp():
    return str(datetime.datetime.utcnow().timestamp()).replace('.', '_')[:-4]


class AudioWriter:
    def __init__(self, output_dir):
        self.output_dir = output_dir
    
    def get_filename(self):
        filename = 'output_{}.wav'.format(stamp())
        return filename

    def flush(self):
        raise NotImplementedError


class WavWriter(AudioWriter):
    def open(self, channels, format, rate):
        self.filepath = self.output_dir + self.get_filename()
        self.wf = wave.open(self.filepath, 'wb')
        self.wf.setnchannels(channels)
        self.wf.setsampwidth(format)
        self.wf.setframerate(rate)
    
    def write(self, content):
        self.wf.writeframes(content)
    
    def close(self):
        self.wf.close()
    
    def flush(self, content, **kwargs):
        self.open(**kwargs)
        self.write(content)
        self.close()
        return self.filepath


class Uploader:
    conversion_format = None
    conversion_extension = None

    @classmethod
    def upload(cls, filepath):
        output_fp = filepath
        if cls.conversion_format:
            output_fp = cls.convert(filepath)
        gz_filepath = output_fp + '.gz'
        cls.compress(output_fp, gz_filepath)
    
    @classmethod
    def convert(cls, source_fp):
        return source_fp

    @staticmethod
    def compress(source_fp, dest_fp):
        with open(source_fp, 'rb') as f_in:
            with gzip.open(dest_fp, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(source_fp)


class MP3Uploader(Uploader):
    conversion_format = 'mp3'
    conversion_extension = '.mp3'

    @classmethod
    def convert(cls, source_fp):
        dest_fp = source_fp.replace('.wav', cls.conversion_extension)
        segment = AudioSegment.from_wav(source_fp)
        segment.export(dest_fp, format=cls.conversion_format)
        
        os.remove(source_fp)
        return dest_fp


class AudioRecorder:
    def __init__(self, chunk=CHUNK, format=FORMAT, width=WIDTH, channels=CHANNELS,
        rate=RATE, time_period=TIME_PERIOD, output_dir=WAVE_OUTPUT_DIR_1):
        self.chunk = chunk
        self.format = format
        self.width = width
        self.channels = channels
        self.rate = rate
        self.time_period = time_period
        self.output_dir = output_dir

        self.audio = pyaudio.PyAudio()
        self.stream = None
    
    def open_stream(self):
        self.stream = self.audio.open(format=self.format,
                channels=self.channels,
                rate=self.rate,
                input=True,
                frames_per_buffer=self.chunk)
    
    def close_stream(self):
        self.stream.stop_stream()
        self.stream.close()
        self.stream = None
    
    def close(self):
        if self.stream:
            self.close_stream()
        self.audio.terminate()
        self.audio = None
    
    def read(self):
        num_frames = int((self.rate / self.chunk) * self.time_period)
        frames = [self.stream.read(self.chunk) for i in range(num_frames)]
        return frames
    
    def flush(self, writer, uploader):
        try:
            frames = self.read()
            content = b''.join(frames)
            filepath = writer.flush(content, channels=self.channels, format=self.audio.get_sample_size(self.format), rate=self.rate)
            uploader.upload(filepath)
        except Exception as e:
            print(e)
            print('\n', str(e))
            self.close()
        # frames = self.read()
        # content = b''.join(frames)
        # filepath = writer.flush(content, channels=self.channels, format=self.audio.get_sample_size(self.format), rate=self.rate)
        # uploader.upload(filepath)

    def start(self, writer, uploader):
        while True:
            if self.audio is None:
                self.audio = pyaudio.PyAudio()
            if self.stream is None:
                self.open_stream()
            self.flush(writer, uploader)


def record():
    recorder = AudioRecorder()
    writer = WavWriter(recorder.output_dir)
    uploader = MP3Uploader()
    
    recorder.start(writer, uploader)


if __name__ == '__main__':
    record()


"""
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
"""
