with open('pages/root', 'w+b') as f:
    f.write(bytearray([1, 0, 0, 0]))
    f.write(bytearray([1, 0, 0, 0, 0, 0, 0, 0]))
    f.write(bytearray([15, 0, 0, 0, 0, 0, 0, 0]))
    s = "I am a test string"
    b = bytearray()
    b.extend(map(ord, s))
    f.write(b)
