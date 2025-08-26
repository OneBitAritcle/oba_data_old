# 📘 개발 협업 가이드

본 문서는 팀원들이 이슈 템플릿 / PR 템플릿을 활용하고, GitHub Flow 기반으로 협업할 수 있도록 정리한 개발 가이드입니다.

### 1. 작업 흐름 (GitHub Flow)
main 브랜치는 항상 배포 가능한 상태를 유지합니다.

새로운 기능/버그 수정은 dev 브랜치에서 진행합니다.

작업 완료 후 Pull Request(PR) 를 생성하고, 리뷰/승인 후 main 브랜치로 병합합니다.

### 2. 브랜치 생성 및 작업
- 최신 main 가져오기
```
git checkout main
git pull origin main
```

- 새 브랜치 생성 (예: 기사 수집 기능 추가)
```
git checkout -b dev/article
```

- 작업 후 커밋:
```
git add .
git commit -m "기사 수집 자동화 DAG 구현"
```

### 3. 원격 저장소로 푸시
```
git push origin dev/article
```

### 4. Pull Request (PR) 생성

GitHub 리포지토리에서 `Compare & pull request` 버튼 클릭

자동으로 적용되는 `PULL_REQUEST_TEMPLATE.md` 에 맞춰 작성

- 주요 작성 항목:
```
PR 타이틀
📌 개요
✨ 주요 변경 사항
💡 변경 이유
🔍 관련 이슈
🧪 테스트 방법
📸 스크린샷/동작 확인
✅ 체크리스트
```

팀원 리뷰 & 승인 후 main 브랜치 병합

### 5. Issue 생성

GitHub → Issues → New Issue

- 템플릿 선택:

  - ⚠️ Issue Report

  - ✨ Pull Request

- 템플릿 항목 채워서 작성:
```
이슈 설명 / 재현 방법 / 기대 동작 / 로그
해결 아이디어 내용 / 기대 효과
원인 추측, 해결 아이디어, 우선순위 등
```

### 6. 템플릿 사용 규칙

반드시 이슈 등록 → 브랜치 작업 → PR 작성 순서 준수

PR에는 반드시 관련 이슈 연결: `close #이슈번호`

PR 병합 전 체크리스트:
```
[ ]로컬 테스트 완료

[ ]필요한 문서 업데이트

[ ]리뷰 승인
 ```

### 7. 작성 예시
#### ⚠️ Issue (Bug Report)
```
⚠️ Issue 설명
로그인 버튼 클릭 시 500 에러 발생

🔁 재현 방법
1. /login 페이지 접속
2. 올바른 계정 입력 후 로그인 시도
3. 500 Internal Server Error 발생

💡 해결 아이디어
JWT 토큰 생성 시 만료 시간 파라미터 누락 가능성
```
#### ✨ Pull Request
```
📌 개요
로그인 API 버그 수정

✨ 변경 사항
- JWT 토큰 생성 로직 수정
- 에러 핸들링 개선

💡 변경 이유
로그인 시 500 에러로 서비스 사용 불가

🔍 관련 이슈
close #12

🧪 테스트 방법
1. `npm run dev` 실행
2. 로그인 시 정상적으로 토큰 발급되는지 확인
```

### 8. 요약

### 1. Issue 템플릿: `문제 제기 & 기능 제안 표준화`

### 2. PR 템플릿: `변경 내역 & 이유 & 테스트 기록`

### 3. GitHub Flow: `main → feature 브랜치 → PR → 리뷰 → 병합`