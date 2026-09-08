import init from './venueflow.js';

const status = document.getElementById('loading');
window.addEventListener('unhandledrejection', () => {
  status.hidden = false;
  status.textContent = '终端未能启动，请检查浏览器控制台或刷新重试。';
});
try {
  await init();
  status.hidden = true;
} catch (error) {
  status.textContent = '终端加载失败。请使用支持 WebGPU 的新版 Chrome 或 Edge。';
  console.error(error);
}
