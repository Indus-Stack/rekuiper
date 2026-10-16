import type { Theme } from 'vitepress';
import DefaultTheme from 'vitepress/theme';
import { inject } from '@vercel/analytics';
import './custom.css';

export default {
  extends: DefaultTheme,
  enhanceApp() {
    // Inject Vercel Analytics
    inject();
  }
} satisfies Theme;
