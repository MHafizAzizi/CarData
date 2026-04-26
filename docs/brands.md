# Mudah.my Brands Reference

## Motorcycles

Available motorcycle brands on Mudah.my. Use with `--category motorcycles --brand <brand>`.

### Popular Brands
- `royal-enfield` — Royal Enfield
- `yamaha` — Yamaha
- `honda` — Honda
- `ktm` — KTM
- `kawasaki` — Kawasaki
- `suzuki` — Suzuki
- `harley-davidson` — Harley-Davidson
- `triumph` — Triumph
- `vespa` — Vespa
- `bmw` — BMW
- `sym` — SYM
- `skyteam` — Skyteam
- `kymco` — Kymco

### All Motorcycle Brands
A, ADIVA, AFAZ, AJS, APRILIA, ARIIC, AVETA

B, BENDA, BENELLI, BIMOTA, BKZ, BLUESHARK, BMW, BRIXTON, BUELL

C, CAN-AM, CFMOTO

D, DAELIM, DAIICHI, DEMAK, DUCATI

E, EBIXON, EZI

F, FANTIC

G, GILERA, GPX

H, HANWAY, HARLEY-DAVIDSON, HUSABERG, HUSQVARNA

I, INDIAN

J, JAWA

K, KAMAX, KAWASAKI, KAYO, KEEWAY, KOVE, KTM, KTNS, KYMCO

L, LAMBRETTA, LAVERDA

M, MBP, MBP MORBIDELLI, MLE, MODA, MODENAS, MOMO, MOTO GUZZI, MOTO MORINI, MV AGUSTA, MZ

N, NAZA, NIMOTA, NITRO, NORTON

O, OTTIMO

P, PETRONAS

Q, QJ MOTOR

R, ROYAL ALLOY, ROYAL ENFIELD

S, SCOMADI, SHERCO, SKYTEAM, SM SPORT, STEYR DAIMLER, SUPERLUX, SUZUKI, SYM

T, THUNDER, TM MOTO, TRIUMPH

V, VESPA, VICTORY, VOGE

W, WMOTO

X, X-MOTO, X-WEDGE

Y, YAMAHA, YADEA

Z, ZEEHO, ZERO ENGINEERING, ZESPARII, ZONTES

---

## Cars

Available car brands on Mudah.my. Use with `--category cars --brand <brand>` (default category).

**Note:** For an exhaustive list of car brands, visit:
https://www.mudah.my/malaysia/cars-for-sale

### Popular Brands
- `toyota` — Toyota
- `honda` — Honda
- `nissan` — Nissan
- `mazda` — Mazda
- `hyundai` — Hyundai
- `proton` — Proton
- `perodua` — Perodua
- `bmw` — BMW
- `mercedes-benz` — Mercedes-Benz
- `ford` — Ford
- `volkswagen` — Volkswagen
- `kia` — Kia
- `mitsubishi` — Mitsubishi

### Examples

Scrape all Toyota cars in Selangor:
```bash
python script.py --category cars --state selangor --brand toyota --start 1 --end 50
```

Scrape all Royal Enfield motorcycles nationwide:
```bash
python script.py --category motorcycles --brand royal-enfield --start 1 --end 20
```

Scrape all brands in Johor (cars):
```bash
python script.py --state johor --start 1 --end 100
```
