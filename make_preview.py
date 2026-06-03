"""Animált GIF előnézet a realtime-szerű él charthoz (telefonra)."""
import math, random
from PIL import Image, ImageDraw, ImageFont

W, H = 900, 440
PAD_L, PAD_R, PAD_T, PAD_B = 64, 18, 18, 30
PLOT_W = W - PAD_L - PAD_R
PLOT_H = H - PAD_T - PAD_B

BG = (13, 17, 23)
GRID = (33, 38, 45)
MUTED = (139, 148, 158)
UP = (38, 166, 154)
DOWN = (239, 83, 80)

random.seed(7)

def demo_series(n, start):
    out, v = [], start
    for _ in range(n):
        v += (random.random() - 0.49) * (start * 0.012)
        out.append(max(v, start * 0.2))
    return out

DATA = demo_series(140, 65000)

try:
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 13)
    font_big = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
except Exception:
    font = ImageFont.load_default()
    font_big = font

def fmt(v):
    return f"{v:,.0f}".replace(",", " ")

def lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))

def render_frame(shown, pulse_t):
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img, "RGBA")

    vis = DATA[:max(shown, 2)]
    vmin, vmax = min(vis), max(vis)
    if vmin == vmax:
        vmin -= 1; vmax += 1
    rng = vmax - vmin
    vmin -= rng * 0.08; vmax += rng * 0.08

    n = len(DATA)
    x_at = lambda i: PAD_L + (i / (n - 1)) * PLOT_W
    y_at = lambda v: PAD_T + PLOT_H - ((v - vmin) / (vmax - vmin)) * PLOT_H

    # rács + y címkék
    for r in range(6):
        val = vmin + (r / 5) * (vmax - vmin)
        y = y_at(val)
        d.line([(PAD_L, y), (W - PAD_R, y)], fill=GRID, width=1)
        d.text((PAD_L - 8, y), fmt(val), font=font, fill=MUTED, anchor="rm")

    col = UP if vis[-1] >= vis[0] else DOWN

    pts = [(x_at(i), y_at(v)) for i, v in enumerate(vis)]

    # területkitöltés (gradiens helyett féligátlátszó)
    poly = pts + [(pts[-1][0], PAD_T + PLOT_H), (pts[0][0], PAD_T + PLOT_H)]
    d.polygon(poly, fill=col + (55,))

    # vonal
    d.line(pts, fill=col, width=3, joint="curve")

    # végpont szaggatott vízszintes
    lx, ly = pts[-1]
    x = PAD_L
    while x < lx:
        d.line([(x, ly), (min(x + 5, lx), ly)], fill=col + (120,), width=1)
        x += 10

    # pulzáló live pont
    p = 5 + 3 * abs(math.sin(pulse_t))
    d.ellipse([lx - p - 4, ly - p - 4, lx + p + 4, ly + p + 4], fill=col + (60,))
    d.ellipse([lx - 5, ly - 5, lx + 5, ly + 5], fill=col)

    # fejléc statok
    start_v, end_v = vis[0], vis[-1]
    chg = end_v - start_v
    pct = chg / abs(start_v) * 100 if start_v else 0
    d.text((PAD_L, 2), f"Utolso ar: {fmt(end_v)}", font=font_big, fill=col, anchor="lt")
    sign = "+" if chg >= 0 else ""
    d.text((W - PAD_R, 6), f"{sign}{fmt(chg)}  ({sign}{pct:.2f}%)   {shown}/{n}",
           font=font, fill=col, anchor="rt")
    return img

frames = []
total = len(DATA)
step = 2  # pont/frame
i = 2
fc = 0
while i <= total:
    frames.append(render_frame(i, fc * 0.6))
    i += step
    fc += 1
# tartsuk a végén pár frame-et
for k in range(12):
    frames.append(render_frame(total, fc * 0.6))
    fc += 1

frames[0].save(
    "realtime_chart_preview.gif",
    save_all=True,
    append_images=frames[1:],
    duration=60,
    loop=0,
    optimize=True,
)
print(f"GIF kesz: {len(frames)} frame")
