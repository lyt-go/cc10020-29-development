import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import MultiTransformView from '@/views/MultiTransformView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/multi-transform-datasets',
      name: 'multi-transform',
      component: MultiTransformView,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
