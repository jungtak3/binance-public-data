# 바이낸스 클라인 데이터 다운로더 설정

바이낸스 퍼블릭 데이터 저장소에서 과거 클라인(캔들스틱) 데이터를 다운로드하는 도구임.

## 사전 요구사항

- Python 3.7 이상
- pip (파이썬 패키지 설치기)

## 설치

1. **프로젝트 디렉토리로 이동:**
   ```bash
   cd binance-public-data/python
   ```

2. **필요한 의존성 설치:**
   ```bash
   pip install -r requirements.txt
   ```

   또는 개별 설치:
   ```bash
   pip install pandas numpy fgrequests
   ```

## 사용법

### 기본 사용법

**특정 심볼들의 클라인 데이터 다운로드:**
```bash
python download-kline2.py -t spot -s BTCUSDT ETHUSDT -i 1d
```

**특정 날짜 범위 데이터 다운로드:**
```bash
python download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -endDate 2024-12-31
```

**특정 폴더에 다운로드:**
```bash
python download-kline2.py -t spot -s ETHUSDT -i 1h -folder ./my_data
```

### 명령줄 인수

- `-t, --type`: 거래 타입 (필수)
  - `spot`: 현물 거래
  - `um`: USD-M 선물
  - `cm`: COIN-M 선물

- `-s, --symbols`: 다운로드할 심볼들 (공백으로 구분)
  - 예: `-s BTCUSDT ETHUSDT ADAUSDT`
  - 제공하지 않으면 모든 사용 가능한 심볼 다운로드

- `-i, --intervals`: 클라인 간격들 (공백으로 구분)
  - 사용 가능: `1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1mo`
  - 예: `-i 1h 1d`

- `-startDate`: YYYY-MM-DD 형식의 시작 날짜
- `-endDate`: YYYY-MM-DD 형식의 끝 날짜
- `-folder`: 출력 디렉토리 (기본값: `./downloaded_klines`)
- `-skip-monthly`: 월간 데이터 다운로드 건너뛰기 (0 또는 1, 기본값: 0)
- `-skip-daily`: 일간 데이터 다운로드 건너뛰기 (0 또는 1, 기본값: 0)
- `-c, --checksum`: 체크섬 파일 다운로드 (0 또는 1, 기본값: 0)

### 예제

**2024년 비트코인 일봉 데이터 다운로드:**
```bash
python download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -endDate 2024-12-31
```

**여러 심볼에 여러 간격으로:**
```bash
python download-kline2.py -t spot -s BTCUSDT ETHUSDT ADAUSDT -i 1h 1d -startDate 2024-01-01
```

**선물 데이터 다운로드:**
```bash
python download-kline2.py -t um -s BTCUSDT -i 1h -startDate 2024-01-01
```

**모든 사용 가능한 심볼 다운로드 (주의: 데이터가 많을 수 있음!):**
```bash
python download-kline2.py -t spot -i 1d -startDate 2024-01-01 -endDate 2024-01-31
```

## 출력 구조

스크립트는 다음 디렉토리 구조를 생성함:

```
downloaded_klines/
├── data/
│   └── spot/
│       ├── monthly/
│       │   └── klines/
│       │       └── BTCUSDT/
│       │           └── 1d/
│       │               ├── BTCUSDT-1d-2024-01.csv
│       │               └── BTCUSDT-1d-2024-02.csv
│       └── daily/
│           └── klines/
│               └── BTCUSDT/
│                   └── 1d/
│                       ├── BTCUSDT-1d-2024-01-01.csv
│                       └── BTCUSDT-1d-2024-01-02.csv
└── BTCUSDT_20240101_20241231.csv  # 병합된 파일
```

## 기능

- **자동 파일 병합**: 개별 CSV 파일들이 심볼별 파일로 자동 병합됨
- **점진적 다운로드**: 스크립트가 기존 데이터를 감지하고 새 데이터만 다운로드함
- **배치 처리**: 빠른 다운로드를 위한 비동기 요청 사용
- **데이터 검증**: 타임스탬프 형식 문제 및 데이터 일관성 처리
- **재개 기능**: 중단된 다운로드 재개 가능

## 문제 해결

**임포트 에러:**
- 모든 의존성이 설치되었는지 확인: `pip install -r requirements.txt`

**네트워크 에러:**
- 인터넷 연결 확인
- 특정 날짜 범위에서 일부 파일이 사용 불가할 수 있음
- 일부 파일이 실패해도 스크립트는 다른 파일 다운로드를 계속함

**권한 에러:**
- 출력 디렉토리에 쓰기 권한이 있는지 확인
- 다른 출력 폴더로 시도: `-folder ./data`

## 데이터 형식

다운로드된 CSV 파일은 다음 컬럼들을 포함함:
- `open_time`: 시가 시간 (밀리초)
- `open`: 시가
- `high`: 고가
- `low`: 저가
- `close`: 종가
- `volume`: 거래량
- `close_time`: 종가 시간 (밀리초)
- `quote_volume`: 견적 자산 거래량
- `count`: 거래 수
- `taker_buy_volume`: 테이커 매수 기본 자산 거래량
- `taker_buy_quote_volume`: 테이커 매수 견적 자산 거래량
- `ignore`: 사용하지 않는 필드

## 참고사항

- 일간 데이터는 다음 간격에서 사용 가능: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d
- 월간 데이터는 3d, 1w, 1mo를 포함한 모든 간격에서 사용 가능
- 스크립트가 출력 디렉토리를 자동 생성함
- 큰 다운로드는 상당한 시간과 디스크 공간이 필요할 수 있음