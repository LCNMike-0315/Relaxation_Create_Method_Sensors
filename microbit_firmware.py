# MakeCode Python for micro:bit V2 + Grove Shield v2.0
# P0 = GSR (analog), P1 = ear-clip heart rate (digital pulse), P2 = thermistor (analog)
# Button A = start 2-minute session (auto-stops at 120s), Button B = manual stop

DURATION_MS = 120000
measuring = False
start_t = 0
bpm = 0
last_t = 0

def on_pulse():
    global bpm, last_t
    now = input.running_time()
    if last_t != 0:
        dt = now - last_t
        if 300 < dt < 2000:
            bpm = Math.round(60000 / dt)
    last_t = now
pins.on_pulsed(DigitalPin.P1, PulseValue.HIGH, on_pulse)

def stop_session():
    global measuring
    if not measuring:
        return
    measuring = False
    serial.write_line("#STOP")
    basic.show_icon(IconNames.YES)

def on_button_pressed_a():
    global measuring, start_t, bpm, last_t
    if measuring:
        return
    bpm = 0
    last_t = 0
    start_t = input.running_time()
    measuring = True
    serial.write_line("#START")
    basic.show_icon(IconNames.SMALL_HEART)
input.on_button_pressed(Button.A, on_button_pressed_a)

def on_button_pressed_b():
    stop_session()
input.on_button_pressed(Button.B, on_button_pressed_b)

def on_forever():
    global bpm
    if measuring:
        now = input.running_time()
        if now - start_t >= DURATION_MS:
            stop_session()
            return

        gsr = pins.analog_read_pin(AnalogPin.P0)
        temp_raw = pins.analog_read_pin(AnalogPin.P2)
        if last_t != 0 and now - last_t > 3000:
            bpm = 0
        serial.write_line(str(now) + "," + str(gsr) + "," + str(bpm) + "," + str(temp_raw))
        basic.pause(200)
    else:
        basic.pause(50)
basic.forever(on_forever)

basic.show_icon(IconNames.YES)