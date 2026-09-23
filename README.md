# پنل آموزشی — نسخه Node.js (Railway / RunFlare)

این پوشه همان پنل را با یک لایه‌ی سازگاری روی Node.js اجرا می‌کند، بدون اینکه
کد اصلی (`worker-src.js`، همان فایل Cloudflare Worker) تغییر کرده باشد.

## فایل‌ها
- `worker-src.js` — کد اصلی پنل، دقیقاً همان index.js شما (دست‌نخورده).
- `server.js` — لایه‌ی سازگاری: KV → Redis، Durable Object (کلاس آنلاین/وبینار/
  تماس تعاملی/تخته) → یک نمونه‌ی درون‌حافظه‌ای، WebSocket → کتابخانه‌ی `ws`.
- `package.json` — وابستگی‌ها (`ws`, `ioredis`).

## متغیرهای محیطی (Environment Variables)
| نام | ضروری؟ | توضیح |
|---|---|---|
| `REDIS_URL` | **بله (برای production)** | آدرس Redis، مثل `redis://...` یا `rediss://...` (Upstash). اگر تنظیم نشود، داده‌ها فقط در RAM نگه‌داری می‌شوند و با هر ری‌استارت پاک می‌شوند — فقط برای تست محلی مناسب است. |
| `GEMINI_API_KEY` | خیر | برای بخش‌های هوش مصنوعی مبتنی بر Gemini |
| `GROQ_API_KEY` / `GROQ_MODEL` | خیر | برای چت هوش مصنوعی با Groq |
| `TOKENHARBOR_API_KEY` / `TOKENHARBOR_MODEL` | خیر | ارائه‌دهنده‌ی جایگزین هوش مصنوعی |
| `PORT` | خیر | هم Railway و هم RunFlare این را خودشان تنظیم می‌کنند |

توکن‌های تلگرام/بله/روبیکا/پیامک نیازی به env var ندارند — از داخل خودِ پنل
(تنظیمات → اطلاع‌رسانی) ذخیره می‌شوند و در همان Redis می‌مانند.

## دیپلوی روی Railway
1. یک پروژه‌ی جدید بسازید و این پوشه را (یا ریپوی گیت‌هاب حاوی آن) وصل کنید.
2. یک سرویس Redis به همان پروژه اضافه کنید (Add → Database → Redis)؛ Railway
   خودش متغیر Redis را می‌سازد — مقدارش را در سرویس اصلی به‌عنوان `REDIS_URL`
   ست کنید (یا اسم متغیر Railway را با `REDIS_URL` یکی کنید).
3. متغیرهای هوش مصنوعی (اختیاری) را در تنظیمات سرویس اضافه کنید.
4. Railway به‌صورت خودکار `npm install` و `npm start` را اجرا می‌کند.
5. **مهم:** تعداد replica/instance سرویس را روی ۱ نگه دارید (چون کلاس آنلاین/
   وبینار در حافظه‌ی همان یک پردازه نگه‌داری می‌شود).

## دیپلوی روی RunFlare
1. پروژه را به‌صورت یک اپ Node.js (نه Static/PHP) بسازید و همین پوشه را آپلود/
   وصل کنید.
2. یک نمونه Redis وصل کنید (سرویس داخلی RunFlare یا یک Redis بیرونی مثل
   Upstash) و آدرسش را در `REDIS_URL` بگذارید.
3. دستور اجرا: `npm install && npm start` (یا Start Command: `node server.js`).
4. اینجا هم تعداد instance را ۱ نگه دارید.

## تست محلی
```bash
npm install
REDIS_URL=redis://localhost:6379 PORT=3000 npm start
# یا بدون Redis (فقط برای تست سریع، داده‌ها موقتی‌اند):
PORT=3000 npm start
```
سپس به `http://localhost:3000/teacher` بروید.
