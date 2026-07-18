import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://memoree.dev',
  integrations: [
    sitemap(),
    starlight({
      title: 'Memoree',
      description: 'Local, evidence-backed memory for machine agents.',
      disable404Route: true,
      favicon: '/favicon.svg',
      logo: {
        src: './src/assets/mark.svg',
        replacesTitle: false,
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/devcited/memoree' },
      ],
      editLink: {
        baseUrl: 'https://github.com/devcited/memoree/edit/main/',
      },
      customCss: ['./src/styles/starlight.css'],
      head: [
        { tag: 'meta', attrs: { name: 'theme-color', content: '#11130f' } },
        { tag: 'meta', attrs: { property: 'og:site_name', content: 'Memoree' } },
      ],
      sidebar: [
        { label: 'Documentation', link: '/docs/' },
        {
          label: 'Use Memoree',
          items: [
            { label: 'Install', link: '/install/' },
            { label: 'Context & configuration', link: '/docs/configuration/' },
            { label: 'Agent integration', link: '/docs/model-integration/' },
            { label: 'Session checkpoints', link: '/docs/checkpoints/' },
          ],
        },
        {
          label: 'Understand the system',
          items: [
            { label: 'Architecture', link: '/docs/architecture/' },
            { label: 'Protocol', link: '/docs/protocol/' },
            { label: 'Quality gates', link: '/docs/quality/' },
            { label: 'Retrieval evaluation', link: '/docs/evaluation/' },
          ],
        },
      ],
    }),
  ],
});
