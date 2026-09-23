import math, os, sys
from fontTools.ttLib import TTFont
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.recordingPen import RecordingPen
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.transformPen import TransformPen

OUT = sys.argv[1]
SEMI = "/usr/share/fonts/truetype/ibm-plex/IBMPlexSans-SemiBold.ttf"
BOLD = "/usr/share/fonts/truetype/ibm-plex/IBMPlexSans-Bold.ttf"
DARK, LIGHT = "#1f2937", "#e5e7eb"
AMBER, BLUE = "#f59e0b", "#326ce5"
TRACK = -5

def r(v):
    return f"{v:.2f}".rstrip("0").rstrip(".")

class Face:
    def __init__(self, path):
        self.font = TTFont(path)
        self.upm = self.font["head"].unitsPerEm
        self.gs = self.font.getGlyphSet()
        self.cmap = self.font.getBestCmap()
        self.hmtx = self.font["hmtx"]
        self.S = 100 / self.upm
    def glyph(self, ch):
        return self.gs[self.cmap[ord(ch)]]
    def adv(self, ch):
        return self.hmtx[self.cmap[ord(ch)]][0] * self.S
    def path(self, ch, x=0, y=0, scale=1.0):
        pen = SVGPathPen(self.gs, ntos=r)
        s = self.S * scale
        self.glyph(ch).draw(TransformPen(pen, (s, 0, 0, -s, x, y)))
        return pen.getCommands()
    def bounds(self, ch, x=0, y=0, scale=1.0):
        bp = BoundsPen(self.gs); self.glyph(ch).draw(bp)
        b = bp.bounds; s = self.S * scale
        return (b[0]*s + x, -b[3]*s + y, b[2]*s + x, -b[1]*s + y)
    def contours(self, ch):
        rp = RecordingPen(); self.glyph(ch).draw(rp)
        cs, cur = [], []
        for op, args in rp.value:
            cur.append((op, args))
            if op in ("closePath", "endPath"):
                cs.append(cur); cur = []
        boxes = []
        for c in cs:
            bp = BoundsPen(self.gs)
            for op, args in c:
                getattr(bp, op)(*args)
            b = bp.bounds; s = self.S
            boxes.append((b[0]*s, -b[3]*s, b[2]*s, -b[1]*s))
        boxes.sort(key=lambda b: b[2]-b[0], reverse=True)
        return boxes

semi = Face(SEMI); bold = Face(BOLD)

def union(a, b):
    if a is None: return b
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))

def word(face, text, x0=0, skip=()):
    """Returns (path d of drawn glyphs, bounds, slots{ch: x}, end x)."""
    x = x0; d = []; b = None; slots = {}
    for ch in text:
        if ch in skip:
            slots[ch] = x
        else:
            d.append(face.path(ch, x))
            b = union(b, face.bounds(ch, x))
        x += face.adv(ch) + TRACK
    return " ".join(d), b, slots, x - TRACK

def stem_widths(face, ch="o"):
    o, i = face.contours(ch)[:2]
    return ((o[2]-o[0]) - (i[2]-i[0])) / 2, ((o[3]-o[1]) - (i[3]-i[1])) / 2

def svg(elements, bounds, name, pad=6):
    minx, miny, maxx, maxy = bounds
    minx -= pad; miny -= pad; maxx += pad; maxy += pad
    w, h = maxx - minx, maxy - miny
    body = "\n".join("  " + e for e in elements)
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="{r(minx)} {r(miny)} {r(w)} {r(h)}" width="{r(w*3)}" height="{r(h*3)}" role="img" aria-labelledby="t">
  <title id="t">{name}</title>
  <style>
    .w {{ fill: {DARK}; }}
    .ws {{ stroke: {DARK}; }}
    @media (prefers-color-scheme: dark) {{ .w {{ fill: {LIGHT}; }} .ws {{ stroke: {LIGHT}; }} }}
  </style>
{body}
</svg>
'''

concepts = {}

# ---- Concept 1: lasso o (wordmark) ----
d, b, slots, end = word(semi, "drover", skip="o")
o_out, o_in = semi.contours("o")[:2]
ox = slots["o"]
cx = ox + (o_out[0] + o_out[2]) / 2
cy = (o_out[1] + o_out[3]) / 2
rx = ((o_out[2]-o_out[0]) + (o_in[2]-o_in[0])) / 4
ry = ((o_out[3]-o_out[1]) + (o_in[3]-o_in[1])) / 4
th, tv = stem_widths(semi)
t = th * 0.95
a = math.radians(122)
p0 = (cx + rx*math.cos(a), cy + ry*math.sin(a))
a = math.radians(112)
p0 = (cx + rx*math.cos(a), cy + ry*math.sin(a))
tail = f"M{r(p0[0])} {r(p0[1])} C{r(p0[0]-1)} {r(p0[1]+18)} {r(cx-rx+2)} {r(cy+ry+30)} {r(cx-rx-12)} {r(cy+ry+30)}"
lasso = [f'<ellipse cx="{r(cx)}" cy="{r(cy)}" rx="{r(rx)}" ry="{r(ry)}"/>', f'<path d="{tail}"/>']
els = [f'<path class="w" d="{d}"/>',
       f'<g fill="none" stroke="{AMBER}" stroke-width="{r(t)}" stroke-linecap="round" stroke-linejoin="round">'] + ["  " + e for e in lasso] + ["</g>"]
lb = union(b, (cx-rx-24, cy-ry-t, cx+rx+t, cy+ry+40))
concepts["concept1-horizontal"] = svg(els, lb, "drover")
# icon: the lasso alone, recentred
ib = (cx-rx-24, cy-ry-t, cx+rx+t, cy+ry+40)
els = [f'<g fill="none" stroke="{AMBER}" stroke-width="{r(t)}" stroke-linecap="round" stroke-linejoin="round">'] + ["  " + e for e in lasso] + ["</g>"]
concepts["concept1-icon"] = svg(els, ib, "drover", pad=8)

# ---- Concept 2: rocking D (cattle brand) + wordmark ----
def rocking_d(x, y, size):
    """Mark in a size x size box at x,y. Returns (elements, bounds)."""
    s = size / 100
    Db = bold.bounds("D")
    dh = Db[3] - Db[1]; dw = Db[2] - Db[0]
    sc = 54 / dh * s
    dpath = bold.path("D", x=x + 50*s - (Db[0] + dw/2)*sc, y=y + 12*s - Db[1]*sc, scale=sc)
    rocker = f'M{r(x+16*s)} {r(y+76*s)} Q{r(x+50*s)} {r(y+98*s)} {r(x+84*s)} {r(y+76*s)}'
    els = [f'<path class="w" d="{dpath}"/>',
           f'<path d="{rocker}" fill="none" stroke="{AMBER}" stroke-width="{r(9*s)}" stroke-linecap="round"/>']
    return els, (x, y, x+size, y+size)

d, b, _, end = word(semi, "drover")
mark_size = 82
mx = b[0] - mark_size - 16
my = b[3] - mark_size + 2
els, mb = rocking_d(mx, my, mark_size)
els.append(f'<path class="w" d="{d}"/>')
concepts["concept2-horizontal"] = svg(els, union(b, mb), "drover")
els, mb = rocking_d(0, 0, 100)
concepts["concept2-icon"] = svg(els, mb, "drover", pad=4)

# ---- Concept 3: hex-d (Kubernetes hexagon as the bowl of the d) ----
def hexagon(cx, cy, rad):
    pts = [(cx + rad*math.cos(math.radians(90 + 60*i)), cy + rad*math.sin(math.radians(90 + 60*i))) for i in range(6)]
    return " ".join(f"{r(px)},{r(py)}" for px, py in pts)

d, b, slots, end = word(semi, "drover", skip="d")
d_out, d_in = semi.contours("d")[:2]
dx = slots["d"]
xh = d_in[3] - d_in[1] + 2*tv  # x-height from the bowl
bowl_cx = dx + (d_in[0] + d_in[2]) / 2 - 1
bowl_cy = (d_out[3] + d_in[1]) / 2  # bowl vertical centre: between baseline overshoot and inner top
rad = (xh - th) / 2
stem_x = bowl_cx + rad*math.cos(math.radians(30))
top = d_out[1]
els = [f'<polygon points="{hexagon(bowl_cx, bowl_cy, rad)}" fill="none" stroke="{BLUE}" stroke-width="{r(th)}" stroke-linejoin="round"/>',
       f'<line class="ws" x1="{r(stem_x)}" y1="{r(top)}" x2="{r(stem_x)}" y2="{r(bowl_cy + rad/2)}" stroke-width="{r(th)}"/>',
       f'<path class="w" d="{d}"/>']
hb = (bowl_cx - rad*math.cos(math.radians(30)) - th/2, top, stem_x + th/2, bowl_cy + rad + th/2)
concepts["concept3-horizontal"] = svg(els, union(b, hb), "drover")
# icon
icx, icy, irad, it = 44, 60, 27, 13
isx = icx + irad*math.cos(math.radians(30))
els = [f'<polygon points="{hexagon(icx, icy, irad)}" fill="none" stroke="{BLUE}" stroke-width="{it}" stroke-linejoin="round"/>',
       f'<line class="ws" x1="{r(isx)}" y1="8" x2="{r(isx)}" y2="{r(icy + irad/2)}" stroke-width="{it}"/>']
concepts["concept3-icon"] = svg(els, (icx - irad*math.cos(math.radians(30)) - it/2, 8, isx + it/2, icy + irad + it/2), "drover", pad=6)

# ---- Concept 4: hoofprint + wordmark ----
def hoof(x, y, size):
    s = size / 100
    def P(px, py): return f"{r(x+px*s)} {r(y+py*s)}"
    left = (f"M{P(44,14)} C{P(30,24)} {P(18,44)} {P(22,66)} C{P(24,78)} {P(32,84)} {P(40,82)} "
            f"C{P(46,80)} {P(48,72)} {P(48,60)} L{P(48,34)} C{P(48,24)} {P(46,18)} {P(44,14)} Z")
    right = (f"M{P(56,14)} C{P(70,24)} {P(82,44)} {P(78,66)} C{P(76,78)} {P(68,84)} {P(60,82)} "
             f"C{P(54,80)} {P(52,72)} {P(52,60)} L{P(52,34)} C{P(52,24)} {P(54,18)} {P(56,14)} Z")
    return [f'<path d="{left}" fill="{AMBER}"/>', f'<path d="{right}" fill="{AMBER}"/>'], (x+18*s, y+18*s, x+82*s, y+84*s)

d, b, _, end = word(semi, "drover")
size = 96
hx = b[0] - 64*size/100 - 12 - 18*size/100
hy = b[3] - 84*size/100 + 3
els, hb = hoof(hx, hy, size)
els.append(f'<path class="w" d="{d}"/>')
concepts["concept4-horizontal"] = svg(els, union(b, hb), "drover")
els, hb = hoof(0, 0, 100)
concepts["concept4-icon"] = svg(els, hb, "drover", pad=6)

os.makedirs(OUT, exist_ok=True)
for k, v in concepts.items():
    open(f"{OUT}/drover-logo-{k}.svg", "w").write(v)

# ---- contact sheet ----
import re
def embed(svgtext, x, y, h, fill):
    vb = re.search(r'viewBox="([^"]+)"', svgtext).group(1).split()
    minx, miny, w, hh = map(float, vb)
    s = h / hh
    body = svgtext.split("</style>", 1)[1].rsplit("</svg>", 1)[0]
    body = body.replace('class="w"', f'fill="{fill}"').replace('class="ws"', f'stroke="{fill}"')
    return f'<g transform="translate({r(x)} {r(y)}) scale({r(s)}) translate({r(-minx)} {r(-miny)})">{body}</g>', w * s

names = ["1 Lasso: the o is a lasso loop", "2 Rocking D: a cattle brand", "3 Hex-d: a Kubernetes hexagon as the bowl", "4 Hoofprint"]
W, RH, COL = 1500, 210, 750
parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{RH*4}" viewBox="0 0 {W} {RH*4}">',
         f'<rect width="{COL}" height="{RH*4}" fill="#ffffff"/>', f'<rect x="{COL}" width="{COL}" height="{RH*4}" fill="#0d1117"/>']
for i in range(4):
    y = i * RH
    for col, (bg, fg) in enumerate([("#ffffff", DARK), ("#0d1117", LIGHT)]):
        x0 = col * COL
        parts.append(f'<text x="{x0+24}" y="{y+34}" font-family="DejaVu Sans" font-size="20" fill="{fg}" opacity="0.7">{names[i]}</text>')
        g, w = embed(concepts[f"concept{i+1}-horizontal"], x0 + 24, y + 56, 130, fg)
        parts.append(g)
        g, w = embed(concepts[f"concept{i+1}-icon"], x0 + COL - 24 - 110, y + 56, 110, fg)
        parts.append(g)
    parts.append(f'<line x1="0" y1="{y+RH}" x2="{W}" y2="{y+RH}" stroke="#888" stroke-width="1" opacity="0.4"/>')
parts.append("</svg>")
open(f"{OUT}/sheet.svg", "w").write("\n".join(parts))
print("ok", sorted(concepts))
